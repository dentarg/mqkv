# frozen_string_literal: true

require "securerandom"
require_relative "base"

module MQKV
  # A key/value map where *many* keys share a *single* stream queue,
  # instead of one queue per key like `Store`.
  #
  # Every write appends a message tagged with its key (in a header) to
  # one stream; each process keeps an in-memory `key => value` map that
  # it builds by scanning the stream once at start and then keeps fresh
  # with a single background consumer. This is the right tool for
  # high-cardinality keyspaces — e.g. per-IP rate-limit counters —
  # where `Store` would declare a queue per key and overwhelm the
  # broker.
  #
  # Consistency is eventual and last-write-wins: a write is visible
  # in-process immediately, and propagates to other processes as the
  # broker fans the message out to their consumers (sub-millisecond on
  # a local broker). There is no atomic read-modify-write across
  # processes, so counters built on top are approximate.
  #
  # Memory is bounded to the live key set: per-key TTLs expire entries
  # on read, a background sweeper drops expired entries, and the stream
  # itself is capped with `x-max-age` / `x-max-length-bytes` so the
  # broker truncates old messages.
  class Map < Base
    KEY_HEADER = "__mqkv_key__"
    SYNC_HEADER = "__mqkv_sync__"

    DEFAULT_SWEEP_INTERVAL = 60
    DEFAULT_SYNC_TIMEOUT = 30

    # max_age:          stream-level retention, e.g. "1h" or 3600 (seconds).
    # max_length_bytes: stream-level size cap in bytes.
    # sweep_interval:   seconds between in-memory expired-entry sweeps
    #                   (nil/0 disables the sweeper).
    # sync_timeout:     seconds to wait for the initial stream scan to
    #                   catch up on `start` before serving (nil waits
    #                   indefinitely).
    def initialize(url, name:, prefix: "mqkv", read_timeout: 0.5, connect_timeout: nil,
                   confirm: true, max_age: nil, max_length_bytes: nil,
                   sweep_interval: DEFAULT_SWEEP_INTERVAL, sync_timeout: DEFAULT_SYNC_TIMEOUT,
                   logger: nil)
      super(url, prefix:, read_timeout:, connect_timeout:, confirm:, logger:)
      @name = name
      @max_age = normalize_max_age(max_age)
      @max_length_bytes = max_length_bytes
      @sweep_interval = sweep_interval
      @sync_timeout = sync_timeout
      @map = {}
      @map_mutex = Mutex.new
      @start_mutex = Mutex.new
      @started = false
      @consumer = nil
      @sweeper = nil
    end

    # Loads current state from the stream and starts the background
    # consumer + sweeper. Idempotent and blocks until the initial scan
    # has caught up (bounded by `sync_timeout`). Called lazily by the
    # first get/set/delete; call it explicitly at boot to pay the load
    # cost up front and avoid serving full buckets before state loads.
    def start
      @start_mutex.synchronize do
        return self if @started

        ensure_stream(stream_name, max_age: @max_age, max_length_bytes: @max_length_bytes)
        start_consumer
        start_sweeper
        @started = true
      end
      self
    end

    def get(key)
      start unless @started
      @map_mutex.synchronize { @map[key]&.current_value }
    end

    def set(key, value, ttl: nil)
      start unless @started
      expires_at = ttl ? Process.clock_gettime(Process::CLOCK_REALTIME) + ttl : nil
      headers = { KEY_HEADER => key }
      headers[EXPIRES_HEADER] = expires_at if expires_at
      publish(stream_name, value.to_s, headers: headers)
      @map_mutex.synchronize { @map[key] = CacheEntry.new(value: value.to_s, expires_at: expires_at) }
      log(:debug) { "at=map_set name=#{@name} key=#{key}" }
      nil
    end

    def delete(key)
      start unless @started
      publish_tombstone(stream_name, headers: { KEY_HEADER => key })
      @map_mutex.synchronize { @map.delete(key) }
      log(:debug) { "at=map_delete name=#{@name} key=#{key}" }
      nil
    end

    def exists?(key)
      !get(key).nil?
    end

    # Number of live (non-expired) keys currently held in memory.
    def size
      @map_mutex.synchronize { @map.count { |_, entry| !entry.current_value.nil? } }
    end

    def purge!
      stop_sweeper
      stop_consumer
      conn = @mutex.synchronize { @connection }
      if conn && !conn.closed?
        conn.with_channel { |ch| ch.queue_delete(stream_name) }
        @mutex.synchronize { @declared_streams.delete(stream_name) }
      end
      @map_mutex.synchronize { @map.clear }
      @started = false
    end

    def close
      stop_sweeper
      stop_consumer
      @map_mutex.synchronize { @map.clear }
      @started = false
      super
    end

    private

    def stream_name
      queue_name(@name)
    end

    # A single long-lived consumer from the start of the stream both
    # replays history (building the map) and then keeps delivering live
    # writes — no gap between "load" and "watch". To know when the
    # historical backlog is drained, we publish a sync marker and block
    # until the consumer delivers it back; everything after it is live.
    #
    # The marker is published *before* subscribing, and that ordering
    # matters: a `x-stream-offset: first` consumer attached to a stream
    # with no committed chunk never enters live-tail mode and receives
    # nothing until one is. Publishing the marker first guarantees the
    # stream is non-empty when the consumer attaches.
    def start_consumer
      token = SecureRandom.uuid
      publish(stream_name, "", headers: { SYNC_HEADER => token })

      ch = connection.channel
      ch.basic_qos(CONSUME_PREFETCH)
      synced = ::Queue.new
      consume_ok = ch.basic_consume(stream_name, no_ack: false,
                                    arguments: { "x-stream-offset" => "first" },
                                    worker_threads: 1) do |msg|
        msg.ack
        if sync_token(msg)
          synced.push(true) if sync_token(msg) == token
        else
          apply(msg)
        end
      end
      @consumer = WatchHandle.new(channel: ch, consumer_tag: consume_ok.consumer_tag)

      wait_for_sync(synced)
      log(:debug) { "at=map_load name=#{@name} keys=#{@map.size}" }
    end

    def wait_for_sync(synced)
      if synced.pop(timeout: @sync_timeout).nil?
        log(:info) { "at=map_load name=#{@name} status=sync_timeout seconds=#{@sync_timeout}" }
      end
    end

    def stop_consumer
      handle = @consumer
      @consumer = nil
      return unless handle

      close_consumer(handle)
    rescue StandardError
      nil
    end

    def start_sweeper
      return unless @sweep_interval&.positive?

      @sweeper = Thread.new do
        loop do
          sleep @sweep_interval
          sweep
        end
      end
    end

    def stop_sweeper
      @sweeper&.kill
      @sweeper = nil
    end

    # Drops expired entries so memory tracks the live key set rather
    # than every key ever written within the stream's retention.
    def sweep
      removed = 0
      @map_mutex.synchronize do
        @map.delete_if do |_key, entry|
          expired = entry.current_value.nil?
          removed += 1 if expired
          expired
        end
      end
      log(:debug) { "at=map_sweep name=#{@name} removed=#{removed} keys=#{@map.size}" } if removed.positive?
    end

    # Applies a delivered message to the in-memory map. Tombstones and
    # already-expired messages remove the key entirely (a missing key
    # reads as nil), so the map never accumulates dead entries.
    def apply(msg)
      key = msg_key(msg)
      return unless key

      entry = msg_to_cache_entry(msg)
      @map_mutex.synchronize do
        if entry.current_value.nil?
          @map.delete(key)
        else
          @map[key] = entry
        end
      end
    end

    def msg_key(msg)
      msg.properties&.headers&.fetch(KEY_HEADER, nil)
    end

    def sync_token(msg)
      msg.properties&.headers&.fetch(SYNC_HEADER, nil)
    end

    def normalize_max_age(max_age)
      return nil if max_age.nil?
      return max_age if max_age.is_a?(String)

      "#{max_age.ceil}s"
    end
  end
end
