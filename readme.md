#

It does not work on MacOS :(

https://github.com/crystal-lang/crystal/issues/11761
https://github.com/crystal-lang/distribution-scripts/issues/125

# x86/64

```bash
SELECT 
    event_type,
    COUNT(*) as total_events,
    MIN(id) as first_id,
    MAX(id) as last_id,
    MIN(event_ts) as first_event_time,
    MAX(event_ts) as last_event_time,
    MIN(created_at) as first_created,
    MAX(created_at) as last_created
FROM kiosk_events
GROUP BY event_type
ORDER BY event_type;
```

output:

```bash

```