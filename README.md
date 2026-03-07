# mqkv

A Ruby gem that turns an AMQP message broker (RabbitMQ 3.9+ / LavinMQ) into a key-value store using stream queues.

Each key maps to a dedicated stream queue. The latest message is the current value. Deletes are implemented as tombstone messages.

## Requirements

- Ruby >= 3.3
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
  read_timeout: 0.5,      # Seconds to wait when draining stream (default: 0.5)
  confirm: true,           # Publisher confirms on set/delete (default: true)
  logger: Logger.new($stdout, level: :debug)  # Optional logger (default: nil)
)
```

The `prefix` is combined with the key to form queue names: `{prefix}.{key}`.

Set `confirm: false` for fire-and-forget writes when durability isn't critical (e.g. caching). Skips the confirm round-trip for faster SETs.

Pass a `Logger` instance via `logger:` to enable debug logging. Log output uses logfmt format and covers connections, stream declarations, cache updates, set/get/delete operations, and watcher lifecycle events.

## Preloading (Cached Reads)

Uncached `get` consumes from the stream each time, bounded by `read_timeout`. For read-heavy workloads, `preload` loads keys into an in-memory cache and starts background watchers to keep it fresh:

```ruby
store = MQKV::Store.new("amqp://localhost")

# Load keys into cache at boot
store.preload("user:1", "user:2", "config:theme")

# Reads are now instant (hash lookup, no AMQP round-trip)
store.get("user:1")  # => from cache

# Writes update the cache immediately + publish to the stream
store.set("user:1", "new-value")
store.get("user:1")  # => "new-value"

# Background watchers pick up writes from other connections
# (eventual consistency, typically sub-millisecond on local broker)
```

Pass `max_messages:` to cap how many messages are consumed per key during preload (default: 10,000), preventing OOM on large streams.

Keys that are `set` or `delete`d after preload also get background watchers automatically. Non-preloaded keys fall back to stream consume on `get`.

## How It Works

- **Queues**: Each key gets a durable stream queue (`x-queue-type: stream`)
- **SET**: Publishes a message to the key's queue (with or without publisher confirms)
- **GET**: Returns from cache if preloaded; otherwise consumes from `x-stream-offset: last` and drains
- **DELETE**: Publishes a tombstone (header `__mqkv_deleted__: true`, empty body)
- **EXISTS?**: Delegates to GET, returns boolean
- **HISTORY**: Consumes from `x-stream-offset: first`, accumulates values, tombstones clear history
- **WATCH**: `basic_consume` with `x-stream-offset: next`, yields values in a background thread
- **PRELOAD**: Consumes full stream per key, caches latest value, starts background watchers

The connection is lazy and thread-safe (protected by Mutex). Stream queue declarations are cached to avoid redundant round-trips.

## Performance

```
--- uncached GET (read_timeout impact) ---
  timeout=0.5s     get:     1.8 ops/s (545.9 ms/op)
  timeout=0.05s    get:    10.4 ops/s ( 96.1 ms/op)

--- confirm vs no-confirm SET ---
  confirm: true    set:   891.1 ops/s (  1.1 ms/op)
  confirm: false   set:  1071.8 ops/s (  0.9 ms/op)

--- preload (cached GET) ---
  uncached         get:    10.5 ops/s ( 95.6 ms/op)
  cached           get: 609756.1 ops/s (  0.002 ms/op)
```

Run `ruby examples/benchmark.rb` against a local broker to reproduce.

## CI

GitHub Actions runs unit tests across Ruby 3.3-4.0 and integration tests against LavinMQ. See `.github/workflows/ci.yml`.

## Development

```bash
bundle install

# Unit tests (no broker needed)
bundle exec rake spec

# Integration tests (requires a running broker)
lavinmq --data-dir /tmp/mqkv-test --bind 127.0.0.1 --amqp-port 5672 &
AMQP_URL=amqp://localhost bundle exec rake integration

# Benchmark
ruby examples/benchmark.rb
```

## License

MIT
