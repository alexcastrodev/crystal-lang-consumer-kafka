-- Initialization script for PostgreSQL database
-- This script creates the necessary table for Kafka consumer events

CREATE TABLE IF NOT EXISTS kiosk_events (
  id BIGSERIAL PRIMARY KEY,
  mall_id VARCHAR,
  kiosk_id VARCHAR,
  event_type VARCHAR,
  event_ts TIMESTAMP,
  amount_cents INTEGER,
  total_items INTEGER,
  payment_method VARCHAR,
  status INTEGER,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

-- Create index on event_type for faster queries
CREATE INDEX IF NOT EXISTS idx_kiosk_events_event_type ON kiosk_events(event_type);

-- Create index on created_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_kiosk_events_created_at ON kiosk_events(created_at);

-- Create index on mall_id and kiosk_id for filtering
CREATE INDEX IF NOT EXISTS idx_kiosk_events_mall_kiosk ON kiosk_events(mall_id, kiosk_id);
