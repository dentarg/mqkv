# mqkv

A Ruby gem that turns an AMQP message broker (RabbitMQ 3.9+ / LavinMQ) into a key-value store using stream queues.

Each key maps to a dedicated stream queue. The latest message is the current value. Deletes are implemented as tombstone messages.

## Requirements

- Ruby >= 3.2
- RabbitMQ 3.9+ with streams enabled, or LavinMQ

## Installation

Add to your Gemfile:

```ruby
gem "mqkv"
```

## Usage

```ruby
require "mqkv"

store = MQKV::Store.new("amqp://localhost")

# Set a value
store.set("user:1:name", "Alice")

# Get the current value
store.get("user:1:name")  # => "Alice"

# Check existence
store.exists?("user:1:name")  # => true

# Delete a key (publishes a tombstone)
store.delete("user:1:name")
store.get("user:1:name")  # => nil

# Value history (tombstones clear accumulated history)
store.set("counter", "1")
store.set("counter", "2")
store.set("counter", "3")
store.history("counter")             # => ["1", "2", "3"]
store.history("counter", limit: 2)   # => ["2", "3"]

# Watch for changes (runs callback in a background thread)
handle = store.watch("events") { |value| puts "New: #{value}" }
# ... later ...
store.unwatch(handle)

store.close
```

## Configuration

```ruby
MQKV::Store.new(
  "amqp://localhost",
  prefix: "myapp",       # Queue name prefix (default: "mqkv")
  read_timeout: 1.0       # Seconds to wait when draining stream (default: 0.5)
)
```

The `prefix` is combined with the key to form queue names: `{prefix}.{key}`.

## How It Works

- **Queues**: Each key gets a durable stream queue (`x-queue-type: stream`)
- **SET**: Publishes a message to the key's queue with publisher confirms
- **GET**: Consumes from `x-stream-offset: last`, drains to find the last message
- **DELETE**: Publishes a tombstone (header `__mqkv_deleted__: true`, empty body)
- **EXISTS?**: Delegates to GET, returns boolean
- **HISTORY**: Consumes from `x-stream-offset: first`, accumulates values, tombstones clear history
- **WATCH**: `basic_consume` with `x-stream-offset: next`, yields values in a background thread

The connection is lazy and thread-safe (protected by Mutex). Stream queue declarations are cached to avoid redundant round-trips.

## CI

GitHub Actions runs unit tests across Ruby 3.2-3.4 and integration tests against LavinMQ. See `.github/workflows/ci.yml`.

## Development

```bash
bundle install

# Unit tests (no broker needed)
bundle exec rspec spec/mqkv/

# Integration tests (requires a running broker)
lavinmq --data-dir /tmp/mqkv-test --bind 127.0.0.1 --amqp-port 5672 &
AMQP_URL=amqp://localhost bundle exec rspec spec/integration/
```

## License

MIT
