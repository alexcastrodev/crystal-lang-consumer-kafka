require 'json'
require 'rdkafka'
require 'pg'
require 'logger'
require 'uri'

# Setup logger
logger = Logger.new(STDOUT)
logger.level = Logger::INFO

# Configuration
KAFKA_BROKERS = ENV.fetch('KAFKA_BROKERS', 'localhost:9092')
KAFKA_TOPIC = ENV.fetch('KAFKA_TOPIC', 'jobs')
GROUP_ID = ENV.fetch('KARAFKA_GROUP_ID', 'bench-group')
DATABASE_URL = ENV.fetch('DATABASE_URL', 'postgres://bench:bench@localhost/bench')
BATCH_SIZE = ENV.fetch('KAFKA_BATCH_SIZE', '1000').to_i
POLL_TIMEOUT_MS = ENV.fetch('KAFKA_POLL_MS', '50').to_i

logger.info "Configurando Kafka Consumer (Ruby):"
logger.info "   Bootstrap Servers: #{KAFKA_BROKERS}"
logger.info "   Group ID: #{GROUP_ID}"
logger.info "   Topic: #{KAFKA_TOPIC}"
logger.info "   Batch Size: #{BATCH_SIZE}"
logger.info "   Database: #{DATABASE_URL}"

# Parse database URL
db_uri = URI.parse(DATABASE_URL)
db_config = {
  host: db_uri.host,
  port: db_uri.port || 5432,
  dbname: db_uri.path[1..],
  user: db_uri.user,
  password: db_uri.password
}

# Initialize database connection
db = PG.connect(db_config)
logger.info "Database connected"

# Kafka consumer configuration
kafka_config = {
  :'bootstrap.servers' => KAFKA_BROKERS,
  :'group.id' => GROUP_ID,
  :'auto.offset.reset' => 'earliest',
  :'enable.auto.commit' => true,
  :'auto.commit.interval.ms' => 1000
}

consumer = Rdkafka::Config.new(kafka_config).consumer
consumer.subscribe(KAFKA_TOPIC)

logger.info "Kafka Consumer initialized successfully (Ruby)"
logger.info "Starting Kafka Consumer..."

# Statistics
total_processed = 0
last_log_time = Time.now
last_processed_count = 0
batch = []

# Graceful shutdown
running = true
trap('INT') do
  logger.info "Recebido SIGINT. Encerrando..."
  running = false
end

trap('TERM') do
  logger.info "Recebido SIGTERM. Encerrando..."
  running = false
end

def build_insert_query(rows)
  return nil if rows.empty?

  placeholders = rows.map.with_index do |_, i|
    base = i * 10
    "($#{base + 1}, $#{base + 2}, $#{base + 3}, $#{base + 4}, $#{base + 5}, $#{base + 6}, $#{base + 7}, $#{base + 8}, $#{base + 9}, $#{base + 10})"
  end.join(', ')

  "INSERT INTO kiosk_events (mall_id, kiosk_id, event_type, event_ts, amount_cents, total_items, payment_method, status, created_at, updated_at) VALUES #{placeholders}"
end

def bulk_insert(db, rows, logger)
  return if rows.empty?

  query = build_insert_query(rows)
  values = rows.flat_map do |row|
    [
      row[:mall_id],
      row[:kiosk_id],
      row[:event_type],
      row[:event_ts],
      row[:amount_cents],
      row[:total_items],
      row[:payment_method],
      row[:status],
      row[:created_at],
      row[:updated_at]
    ]
  end

  db.exec_params(query, values)
end

begin
  batch_timeout = Time.now

  while running
    message = consumer.poll(POLL_TIMEOUT_MS)

    if message
      begin
        payload = JSON.parse(message.payload)
        now = Time.now

        event_ts = begin
          payload['event_ts'] ? Time.parse(payload['event_ts']) : now
        rescue
          now
        end

        event_type = payload['event_type'] ? "ruby-#{payload['event_type']}" : 'ruby-unknown'

        row = {
          mall_id: payload['mall_id'],
          kiosk_id: payload['kiosk_id'],
          event_type: event_type,
          event_ts: event_ts,
          amount_cents: payload['amount_cents'],
          total_items: payload['total_items'],
          payment_method: payload['payment_method'],
          status: payload['status'],
          created_at: now,
          updated_at: now
        }

        batch << row
      rescue JSON::ParserError => e
        logger.error "JSON parse error: #{e.message}"
      rescue => e
        logger.error "Processing error: #{e.message}"
      end
    end

    # Process batch when reaching batch size or timeout
    now = Time.now
    time_diff = (now - batch_timeout) * 1000  # Convert to ms

    if batch.size >= BATCH_SIZE || (time_diff >= 1000 && batch.size > 0)
      batch_start = Time.now

      bulk_insert(db, batch, logger)

      total_processed += batch.size
      insert_time_ms = ((Time.now - batch_start) * 1000).round(2)

      # Log performance every 5 seconds
      time_since_log = (Time.now - last_log_time)
      if time_since_log >= 5.0
        new_processed = total_processed - last_processed_count
        rate = (new_processed / time_since_log).round(2)

        logger.info "Performance: #{rate} msgs/sec | Batch: #{batch.size} | Insert: #{insert_time_ms}ms | Total: #{total_processed}"

        last_log_time = Time.now
        last_processed_count = total_processed
      end

      batch.clear
      batch_timeout = Time.now
    end
  end

  # Process remaining messages
  if batch.size > 0
    bulk_insert(db, batch, logger)
    total_processed += batch.size
  end

rescue => e
  logger.error "Error in consumer loop: #{e.message}"
  logger.error e.backtrace.join("\n")
ensure
  logger.info "Final stats: #{total_processed} mensagens processadas"

  begin
    consumer.close
  rescue => e
    logger.warn "Error closing Kafka consumer: #{e.message}"
  end

  begin
    db.close
  rescue => e
    logger.warn "Error closing database: #{e.message}"
  end

  logger.info "Consumer fechado."
end
