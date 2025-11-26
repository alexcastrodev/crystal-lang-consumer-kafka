#

You cannot run it on Mac with Container

https://github.com/crystal-lang/crystal/issues/11761

https://github.com/crystal-lang/distribution-scripts/issues/125

## Workaround

```bash
brew install crystal librdkafka
docker compose -f docker-compose.local.yml up -d
cd crystal
shards install
```

### Build 

```bash
crystal build consumer.cr \
    --release \
    --no-debug \
    -o consumer
```

### Run

```bash
./consumer
```

# x86/64

# Bench

```bash
docker compose exec -it postgres psql -U bench -d bench -c "SELECT event_type, COUNT(*) as total_events, MIN(id) as first_id, MAX(id) as last_id, MIN(event_ts) as first_event_time, MAX(event_ts) as last_event_time, MIN(created_at) as first_created, MAX(created_at) as last_created FROM kiosk_events GROUP BY event_type ORDER BY event_type;"
```

output:

```bash
event_type   |total_events|first_id|last_id|first_event_time       |last_event_time        |first_created          |last_created           |
-------------+------------+--------+-------+-----------------------+-----------------------+-----------------------+-----------------------+
crystal-visit|     1499001|       1|1499001|2024-11-27 22:33:10.728|2025-11-26 22:37:12.403|2025-11-26 22:38:19.131|2025-11-26 22:38:34.185|
ruby-visit   |     1500000| 1499002|2999001|2025-11-26 22:41:46.000|2025-11-26 22:42:08.000|2025-11-26 22:41:46.000|2025-11-26 22:42:08.000|
```

## Time Summary - Processing ~1.5M events

### Crystal Consumer
- **Total events**: 1,499,001
- **Processing time**: ~15 seconds
  - Start: 22:38:19
  - End: 22:38:34
- **Rate**: ~99,933 events/second

### Ruby Consumer
- **Total events**: 1,500,000
- **Processing time**: ~22 seconds
  - Start: 22:41:46
  - End: 22:42:08
- **Rate**: ~68,181 events/second

### Performance Comparison
- **Crystal is ~46% faster** than Ruby
- Crystal processed the same amount of events in **7 seconds less**
- **Throughput ratio**: Crystal is **1.46x faster**

---

## Time Summary - Processing ~50.5M events

```bash
event_type   |total_events|first_id|last_id  |first_event_time       |last_event_time        |first_created          |last_created           |
-------------+------------+--------+---------+-----------------------+-----------------------+-----------------------+-----------------------+
crystal-visit|    50500000|       1| 88421000|2024-11-27 22:51:17.266|2025-11-26 22:56:09.600|2025-11-26 22:51:19.709|2025-11-26 23:00:40.427|
ruby-visit   |    50500000|  726001|101000000|2025-11-26 22:51:27.000|2025-11-26 23:03:50.000|2025-11-26 22:51:27.000|2025-11-26 23:03:50.000|
```

### Crystal Consumer
- **Total events**: 50,500,000
- **Processing time**: ~9 minutes 21 seconds (561 seconds)
  - Start: 22:51:19
  - End: 23:00:40
- **Rate**: ~90,016 events/second

### Ruby Consumer
- **Total events**: 50,500,000
- **Processing time**: ~12 minutes 23 seconds (743 seconds)
  - Start: 22:51:27
  - End: 23:03:50
- **Rate**: ~67,970 events/second

### Performance Comparison
- **Crystal is ~32% faster** than Ruby
- Crystal processed the same amount of events in **~3 minutes less**
- **Throughput ratio**: Crystal is **1.32x faster**