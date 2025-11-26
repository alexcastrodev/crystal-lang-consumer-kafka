using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

var topic = Environment.GetEnvironmentVariable("TOPIC") ?? "jobs";
var bootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS") ?? "kafka:9092";

const int INITIAL_MESSAGES = 1_000_000;
const int BATCH_SIZE = 100_000;
const int BATCH_COUNT = 5;

Console.WriteLine($"[{DateTime.Now}] Starting Phase 1: Producing {INITIAL_MESSAGES:N0} messages to topic: {topic}");

var config = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    Acks = Acks.Leader,
    LingerMs = 5,
    BatchSize = 64 * 1024,
    CompressionType = CompressionType.Lz4,
    QueueBufferingMaxMessages = 1000000,
    QueueBufferingMaxKbytes = 1048576
};

var jsonOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var globalSeq = 0L;

// Phase 1: Initial 1 million messages
ProduceMessages(producer, jsonOptions, topic, INITIAL_MESSAGES, ref globalSeq);
producer.Flush(TimeSpan.FromSeconds(60));

Console.WriteLine($"[{DateTime.Now}] Finished Phase 1: {INITIAL_MESSAGES:N0} messages produced");
Console.WriteLine($"[{DateTime.Now}] Starting Phase 2: {BATCH_SIZE:N0} messages every minute for {BATCH_COUNT} minutes");

// Phase 2: 100k messages every 1 minute for 5 minutes
for (int cycle = 0; cycle < BATCH_COUNT; cycle++)
{
    var cycleStart = DateTime.Now;
    Console.WriteLine($"[{cycleStart}] Starting batch {cycle + 1}/{BATCH_COUNT}");

    ProduceMessages(producer, jsonOptions, topic, BATCH_SIZE, ref globalSeq);
    producer.Flush(TimeSpan.FromSeconds(30));

    var cycleEnd = DateTime.Now;
    Console.WriteLine($"[{cycleEnd}] Completed batch {cycle + 1}/{BATCH_COUNT}");

    // Wait until next minute
    var elapsed = (cycleEnd - cycleStart).TotalSeconds;
    var waitTime = Math.Max(0, 60 - elapsed);

    if (waitTime > 0 && cycle < BATCH_COUNT - 1)
    {
        Console.WriteLine($"[{DateTime.Now}] Waiting {waitTime:F1} seconds until next batch");
        Thread.Sleep(TimeSpan.FromSeconds(waitTime));
    }
}

Console.WriteLine($"[{DateTime.Now}] All batches completed. Total messages: {globalSeq:N0}");

static void ProduceMessages(IProducer<string, string> producer, JsonSerializerOptions jsonOptions, string topic, int count, ref long globalSeq)
{
    for (int i = 0; i < count; i++)
    {
        var seq = Interlocked.Increment(ref globalSeq);
        var kioskId = (int)(seq / 100_000L);
        var eventTime = DateTime.Now.AddDays(-Random.Shared.Next(365));

        var kioskEvent = new KioskEvent
        {
            MallId = 1,
            KioskId = kioskId,
            EventType = "visit",
            EventTs = eventTime.ToString("o"),
            AmountCents = 1000,
            TotalItems = 5,
            PaymentMethod = 1,
            Status = 0
        };

        var json = JsonSerializer.Serialize(kioskEvent, jsonOptions);
        var message = new Message<string, string>
        {
            Key = kioskId.ToString(),
            Value = json
        };

        try
        {
            producer.Produce(topic, message, deliveryReport =>
            {
                if (deliveryReport.Error.IsError)
                {
                    Console.WriteLine($"Error: {deliveryReport.Error.Reason}");
                }
            });
        }
        catch (ProduceException<string, string> ex)
        {
            // If queue is full, wait and retry
            if (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                Thread.Sleep(10);
                i--; // Retry this message
            }
            else
            {
                throw;
            }
        }

        // Log progress every 100k messages
        if (seq % 100_000 == 0)
        {
            Console.WriteLine($"[{DateTime.Now}] Produced {seq:N0} messages");
        }
    }
}

public class KioskEvent
{
    public int MallId { get; set; }
    public int KioskId { get; set; }
    public string EventType { get; set; }
    public string EventTs { get; set; }
    public int AmountCents { get; set; }
    public int TotalItems { get; set; }
    public int PaymentMethod { get; set; }
    public int Status { get; set; }
}
