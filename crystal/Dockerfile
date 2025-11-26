FROM crystallang/crystal:1.18.2-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache \
    git \
    postgresql-dev \
    librdkafka-dev \
    build-base

# Copy shard files
COPY shard.yml ./

# Install dependencies
RUN shards install --ignore-crystal-version

# Copy source code
COPY . .

# Build the application
RUN crystal build consumer.cr \
    --release \
    --no-debug \
    -o consumer

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache \
    postgresql-libs \
    librdkafka \
    gc

# Copy the built binary
COPY --from=builder /app/consumer /app/consumer

# Create non-root user
RUN addgroup -g 1000 crystal && \
    adduser -D -u 1000 -G crystal crystal && \
    chown -R crystal:crystal /app

USER crystal

# Environment variables
ENV KAFKA_BROKERS=kafka:9092
ENV KARAFKA_GROUP_ID=g1
ENV KAFKA_TOPIC=jobs
ENV KAFKA_POLL_MS=50
ENV KAFKA_BATCH_SIZE=1000
ENV KAFKA_AUTO_OFFSET_RESET=earliest
ENV DATABASE_URL=postgres://bench:bench@postgres/bench

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD pgrep -f consumer || exit 1

CMD ["/app/consumer"]
