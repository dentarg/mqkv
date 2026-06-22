# mqkv

A Ruby gem that turns an AMQP message broker (RabbitMQ 3.9+ / LavinMQ) into a key-value store using stream queues.

Two access patterns are provided:

- `MQKV::Store` — each key maps to its own dedicated stream queue. The latest message is the current value; deletes are tombstone messages. Best for a bounded set of keys.
- `MQKV::Map` — many keys share a *single* stream queue, kept in an in-memory map per process. Best for high-cardinality keyspaces (e.g. per-IP counters) where one queue per key would overwhelm the broker.

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

# Set with TTL (seconds) - expires on read, broker cleans up via x-max-age
store.set("session", "abc123", ttl: 3600)
store.get("session")  # => "abc123"
# ... after 1 hour ...
store.get("session")  # => nil

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
  prefix: "myapp",          # Queue name prefix (default: "mqkv")
  read_timeout: 0.5,        # Seconds to wait when draining stream (default: 0.5)
  connect_timeout: 5,       # TCP connect timeout in seconds (default: nil, which
                            # falls back to amqp-client's 30s). Useful at boot to
                            # fail fast on an unreachable broker instead of
                            # blocking the caller for ~30s.
  confirm: true,            # Publisher confirms on set/delete (default: true)
  cache_watchers: true,     # Spawn a background consumer per cached key so
                            # writes from other processes propagate into the
                            # cache. Set to false when this process is the
                            # sole writer — saves one AMQP consumer thread
                            # per preloaded key. Set/delete still update the
                            # cache in-process. (default: true)
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

For keys whose streams accumulate long histories (e.g. append-only event logs or repeated snapshots), use `preload_latest` instead — it consumes from `x-stream-offset: last`, so only the current value is read regardless of history depth:

```ruby
store.preload_latest("oom", "competition:5433509")
```

This is much cheaper at boot when streams hold thousands of messages and callers only care about the latest value. Tombstones are still respected (a tombstone as the latest entry caches as nil). Background watchers behave the same as for `preload`.

Keys that are `set` or `delete`d after preload also get background watchers automatically. Non-preloaded keys fall back to stream consume on `get`.

### Cache-only reads

For callers that must not block on AMQP (e.g. request handlers that should render a "please wait" placeholder when the cache isn't ready), use `cached_get` and `cached?`:

```ruby
store.preload_latest("config:theme")

store.cached_get("config:theme")  # => the cached value
store.cached_get("missing")       # => nil (not in cache; no stream read)
store.cached?("config:theme")     # => true
store.cached?("missing")          # => false
```

`cached_get` returns the cached value if present (or nil for a tombstoned key) and never falls through to the stream. `cached?` lets you distinguish "tombstoned" (cached, value nil) from "not cached".

## How It Works

- **Queues**: Each key gets a durable stream queue (`x-queue-type: stream`)
- **SET**: Publishes a message to the key's queue (with or without publisher confirms). Optional `ttl:` stores an expiration timestamp in a header and declares the stream with `x-max-age` for broker-level cleanup.
- **GET**: Returns from cache if preloaded; otherwise consumes from `x-stream-offset: last` and drains. Expired messages return nil.
- **DELETE**: Publishes a tombstone (header `__mqkv_deleted__: true`, empty body)
- **EXISTS?**: Delegates to GET, returns boolean
- **HISTORY**: Consumes from `x-stream-offset: first`, tombstones clear history. Memory is bounded: only the last `limit` values are retained while scanning, however long the stream is.
- **WATCH**: `basic_consume` with `x-stream-offset: next`, yields values in a background thread
- **PRELOAD**: Scans the full stream per key (streaming, only the last message is retained), caches latest value, starts background watchers

The connection is lazy and thread-safe (protected by Mutex). Stream queue declarations are cached to avoid redundant round-trips. Stream scans ack in batches as they progress, so streams longer than the consumer prefetch (256) drain fully instead of stalling at the first flow-control window.

## Map (many keys, one queue)

`MQKV::Store` declares one stream queue per key. For high-cardinality keyspaces — e.g. a rate-limit counter per client IP — that means one queue per key, which overwhelms the broker. `MQKV::Map` instead stores *all* keys in a single stream (the key travels in a message header) and keeps a process-local `key => value` map fresh with one background consumer:

```ruby
require "mqkv"

# One stream queue named "mqkv.ratelimit" holds every key.
map = MQKV::Map.new(
  "amqp://localhost",
  name: "ratelimit",
  max_age: "1h",       # broker truncates messages older than this
  confirm: false,      # fire-and-forget writes (caching, not durability)
)

# Loads current state from the stream and starts the background
# consumer + sweeper. Called lazily on first use; call at boot to pay
# the load cost up front.
map.start

map.set("1.2.3.4", "5", ttl: 3600)
map.get("1.2.3.4")   # => "5" (in-memory lookup, no round-trip)
map.set("5.6.7.8", "2")
map.size             # => 2 (live keys held in memory)

map.delete("1.2.3.4")
map.get("1.2.3.4")   # => nil

map.close
```

### Configuration

```ruby
MQKV::Map.new(
  "amqp://localhost",
  name: "ratelimit",        # stream name suffix (required)
  prefix: "mqkv",           # queue is "{prefix}.{name}" (default: "mqkv")
  max_age: "1h",            # x-max-age retention; accepts "1h" or seconds (default: nil)
  max_length_bytes: nil,    # x-max-length-bytes size cap (default: nil)
  read_timeout: 0.5,        # drain idle window in seconds (default: 0.5)
  confirm: true,            # publisher confirms on set/delete (default: true)
  sweep_interval: 60,       # seconds between in-memory expired-entry sweeps (default: 60)
  sync_timeout: 30,         # max seconds to wait for the initial scan on start (default: 30)
  logger: nil,              # optional logfmt logger (default: nil)
)
```

### How it works

- **One queue**: All keys live in `{prefix}.{name}`, declared with `x-max-age` / `x-max-length-bytes` so the broker truncates old messages.
- **SET / DELETE**: Publishes a message tagged with header `__mqkv_key__`; the local map is updated immediately so the writer reads its own writes without lag. `delete` publishes a tombstone.
- **GET**: In-memory hash lookup. Per-key TTLs (header `__mqkv_expires_at__`) expire entries on read.
- **start / load**: A single consumer reads the stream from the beginning to rebuild the map, then keeps delivering live writes from other processes. A sync marker is published *before* subscribing — it marks the catch-up point and primes the stream, since a `x-stream-offset: first` consumer attached to a stream with no committed chunk never enters live-tail mode and receives nothing until one exists.
- **Bounded memory**: A background sweeper drops expired entries, so memory tracks the live key set rather than every key ever written within the stream's retention.

### Consistency

Writes are visible **in-process immediately** and propagate to other processes as the broker fans the message out to their consumers (sub-millisecond on a local broker). There is no atomic read-modify-write across processes — it is last-write-wins, eventually consistent. Counters built on top (rate limiters, etc.) are therefore **approximate** across processes: under contention a value can briefly over- or under-count. That is the standard trade-off for a distributed counter without server-side atomic operations.

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
