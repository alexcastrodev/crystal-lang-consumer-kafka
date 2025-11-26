require "json"
require "pg"
require "db"
require "log"
require "crafka"

module KafkaConsumer
  Log = ::Log.for("kafka.consumer")

  # Database model
  struct KioskEvent
    include JSON::Serializable

    property mall_id : Int32?
    property kiosk_id : Int32?
    property event_type : String?
    property event_ts : String?
    property amount_cents : Int32?
    property total_items : Int32?
    property payment_method : Int32?
    property status : Int32?
  end

  class Config
    property bootstrap_servers : String
    property group_id : String
    property topic : String
    property poll_ms : Int32
    property batch_size : Int32
    property auto_offset_reset : String
    property db_url : String

    def initialize
      @bootstrap_servers = ENV.fetch("KAFKA_BROKERS", "localhost:9092")
      @group_id = ENV.fetch("KARAFKA_GROUP_ID", "g1")
      @topic = ENV.fetch("KAFKA_TOPIC", "jobs")
      @poll_ms = ENV.fetch("KAFKA_POLL_MS", "50").to_i
      @batch_size = ENV.fetch("KAFKA_BATCH_SIZE", "1000").to_i
      @auto_offset_reset = ENV.fetch("KAFKA_AUTO_OFFSET_RESET", "earliest")
      @db_url = ENV.fetch("DATABASE_URL", "postgres://bench:bench@localhost/bench")
    end

    def display
      Log.info { "Configurando Kafka Consumer (Crystal):" }
      Log.info { "   Bootstrap Servers: #{@bootstrap_servers}" }
      Log.info { "   Group ID: #{@group_id}" }
      Log.info { "   Topic: #{@topic}" }
      Log.info { "   Batch Size: #{@batch_size}" }
      Log.info { "   Database: #{@db_url}" }
    end
  end

  class Consumer
    @config : Config
    @db : DB::Database
    @kafka_consumer : Kafka::Consumer
    @running : Bool
    @total_processed : Int64
    @last_log_time : Time
    @last_processed_count : Int64

    def initialize
      @config = Config.new
      @config.display

      @db = DB.open(@config.db_url)

      # Initialize Kafka consumer
      kafka_config = {
        "bootstrap.servers" => @config.bootstrap_servers,
        "group.id" => @config.group_id,
        "auto.offset.reset" => @config.auto_offset_reset,
        "enable.auto.commit" => "true",
        "auto.commit.interval.ms" => "1000"
      }
      @kafka_consumer = Kafka::Consumer.new(kafka_config)
      @kafka_consumer.subscribe(@config.topic)

      @running = true
      @total_processed = 0_i64
      @last_log_time = Time.utc
      @last_processed_count = 0_i64

      setup_signal_handlers

      Log.info { "Kafka Consumer initialized successfully (Crystal)" }
    end

    def start
      Log.info { "Starting Kafka Consumer..." }

      batch = [] of Kafka::Message
      batch_timeout = Time.monotonic

      @kafka_consumer.each do |message|
        break unless @running

        batch << message

        # Process batch when reaching batch size or timeout
        now = Time.monotonic
        if batch.size >= @config.batch_size || (now - batch_timeout).total_milliseconds >= 1000
          process_batch(batch)
          batch.clear
          batch_timeout = now
        end
      end

      # Process remaining messages
      process_batch(batch) unless batch.empty?

    rescue ex : Exception
      Log.error { "Error in consumer loop: #{ex.message}" }
      Log.error { ex.backtrace.join("\n") }
    ensure
      shutdown
    end

    private def process_batch(messages : Array(Kafka::Message))
      return if messages.empty?

      batch_start = Time.monotonic
      rows = [] of Hash(Symbol, DB::Any)

      messages.each do |message|
        begin
          payload = String.new(message.payload)
          event = KioskEvent.from_json(payload)
          rows << build_row(event)
        rescue ex : JSON::ParseException
          Log.error { "JSON parse error: #{ex.message}" }
        rescue ex
          Log.error { "Processing error: #{ex.message}" }
        end
      end

      if rows.size > 0
        begin
          insert_start = Time.monotonic
          bulk_insert(rows)
          insert_time = Time.monotonic - insert_start

          @total_processed += rows.size
          log_performance(insert_time.total_milliseconds, rows.size)

        rescue ex
          Log.error { "Batch insert failed: #{ex.message}" }
          Log.error { ex.backtrace.join("\n") }
        end
      end
    end

    private def build_row(event : KioskEvent) : Hash(Symbol, DB::Any)
      now = Time.utc

      event_ts = if ts = event.event_ts
        begin
          Time.parse_rfc3339(ts)
        rescue
          now
        end
      else
        now
      end

      event_type = if et = event.event_type
        "crystal-#{et}"
      else
        "crystal-unknown"
      end

      Hash(Symbol, DB::Any).new.tap do |hash|
        hash[:mall_id] = event.mall_id
        hash[:kiosk_id] = event.kiosk_id
        hash[:event_type] = event_type
        hash[:event_ts] = event_ts
        hash[:amount_cents] = event.amount_cents
        hash[:total_items] = event.total_items
        hash[:payment_method] = event.payment_method
        hash[:status] = event.status
        hash[:created_at] = now
        hash[:updated_at] = now
      end
    end

    private def bulk_insert(rows : Array(Hash(Symbol, DB::Any)))
      return if rows.empty?

      # Build bulk insert query
      placeholders = rows.map_with_index do |_, i|
        base = i * 10
        "(
          $#{base + 1}, $#{base + 2}, $#{base + 3}, $#{base + 4}, $#{base + 5},
          $#{base + 6}, $#{base + 7}, $#{base + 8}, $#{base + 9}, $#{base + 10}
        )"
      end.join(", ")

      sql = <<-SQL
        INSERT INTO kiosk_events
          (mall_id, kiosk_id, event_type, event_ts, amount_cents,
           total_items, payment_method, status, created_at, updated_at)
        VALUES #{placeholders}
      SQL

      # Flatten all values
      args = rows.flat_map do |row|
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
          row[:updated_at],
        ]
      end

      @db.exec(sql, args: args)
    end

    private def log_performance(insert_time_ms : Float64, batch_size : Int32)
      now = Time.utc
      time_diff = (now - @last_log_time).total_seconds

      if time_diff >= 5.0 # Log every 5 seconds
        new_processed = @total_processed - @last_processed_count
        rate = (new_processed / time_diff).round(2)

        Log.info {
          "Performance: #{rate} msgs/sec | Batch: #{batch_size} | " \
          "Insert: #{insert_time_ms.round(2)}ms | Total: #{@total_processed}"
        }

        @last_log_time = now
        @last_processed_count = @total_processed
      end
    end

    private def setup_signal_handlers
      Signal::INT.trap do
        Log.info { "Recebido SIGINT. Encerrando..." }
        @running = false
      end

      Signal::TERM.trap do
        Log.info { "Recebido SIGTERM. Encerrando..." }
        @running = false
      end
    end

    private def shutdown
      Log.info { "Final stats: #{@total_processed} mensagens processadas" }

      begin
        @kafka_consumer.close
      rescue ex
        Log.warn { "Error closing Kafka consumer: #{ex.message}" }
      end

      begin
        @db.close
      rescue ex
        Log.warn { "Error closing database: #{ex.message}" }
      end

      Log.info { "Consumer fechado." }
    end
  end
end

# Main entry point
consumer = KafkaConsumer::Consumer.new
consumer.start
