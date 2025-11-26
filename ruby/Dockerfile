# Build stage
FROM ruby:3.4.7-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache \
    build-base \
    postgresql-dev \
    librdkafka-dev \
    git \
    bash

# Copy Gemfile
COPY Gemfile ./

# Install gems
RUN bundle install

# Runtime stage
FROM ruby:3.4.7-alpine

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache \
    postgresql-libs \
    librdkafka \
    tzdata

# Copy gems from builder
COPY --from=builder /usr/local/bundle /usr/local/bundle

# Copy application
COPY consumer.rb .

CMD ["ruby", "consumer.rb"]
