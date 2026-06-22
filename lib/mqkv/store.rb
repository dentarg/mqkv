# frozen_string_literal: true

require_relative "base"

module MQKV
  # Key/value store where each key maps to its own stream queue. The
  # latest message on a key's stream is its current value; deletes are
  # tombstone messages. Optional in-memory caching with per-key
  # background watchers keeps reads instant for preloaded keys.
  #
  # For keyspaces with very high cardinality (e.g. one entry per client
  # IP), prefer `MQKV::Map`, which holds many keys in a single stream
  # and avoids declaring one queue per key.
  class Store < Base
    def initialize(url, prefix: "mqkv", read_timeout: 0.5, connect_timeout: nil, confirm: true, cache_watchers: true, logger: nil)
      super(url, prefix:, read_timeout:, connect_timeout:, confirm:, logger:)
      @cache_watchers_enabled = cache_watchers
      @cache = nil
      @cache_mutex = Mutex.new
      @cache_watchers = {}
    end

    def set(key, value, ttl: nil)
      name = queue_name(key)
      max_age = ttl ? "#{ttl.ceil}s" : nil
      ensure_stream(name, max_age: max_age)
      expires_at = ttl ? Process.clock_gettime(Process::CLOCK_REALTIME) + ttl : nil
      headers = expires_at ? { EXPIRES_HEADER => expires_at } : nil
      publish(name, value.to_s, headers: headers)
      log(:debug) { "at=set key=#{key} queue=#{name}" }
      if @cache
        @cache_mutex.synchronize { @cache[key] = CacheEntry.new(value: value.to_s, expires_at: expires_at) }
        log(:debug) { "at=set key=#{key} cache=updated" }
        start_cache_watcher(key)
      end
      nil
    end

    def get(key)
      if @cache
        @cache_mutex.synchronize do
          if @cache.key?(key)
            log(:debug) { "at=get key=#{key} source=cache" }
            return @cache[key].current_value
          end
        end
      end
      log(:debug) { "at=get key=#{key} source=stream" }
      resolve_current(consume_stream(queue_name(key), offset: "last"))
    end

    # Cache-only read. Returns the cached value, or nil if the key has
    # been tombstoned or isn't in the cache at all. Never falls through
    # to the stream — intended for callers that want to decouple read
    # latency from the broker (e.g. an HTTP handler that should show a
    # placeholder when the cache isn't ready yet rather than block on
    # AMQP). Use `cached?(key)` to distinguish "tombstoned" from
    # "not cached".
    def cached_get(key)
      return nil unless @cache

      @cache_mutex.synchronize do
        return @cache[key].current_value if @cache.key?(key)
      end
      nil
    end

    # Whether the key is present in the in-memory cache. A tombstoned
    # key still counts as cached (cached_get returns nil for it).
    # Returns false if no preload has happened yet (no cache exists).
    def cached?(key)
      return false unless @cache

      @cache_mutex.synchronize { @cache.key?(key) }
    end

    def delete(key)
      name = queue_name(key)
      ensure_stream(name)
      publish_tombstone(name)
      log(:debug) { "at=delete key=#{key} queue=#{name}" }
      if @cache
        @cache_mutex.synchronize { @cache[key] = CacheEntry.new(value: nil, expires_at: nil) }
        log(:debug) { "at=delete key=#{key} cache=tombstoned" }
        start_cache_watcher(key)
      end
      nil
    end

    def exists?(key)
      !get(key).nil?
    end

    def history(key, limit: 10)
      values = []
      consume_stream(queue_name(key), offset: "first") do |msg|
        if tombstone?(msg)
          values.clear
        else
          values << msg.body
          # Only the last `limit` values can ever be returned, so drop
          # older ones as we scan — memory stays O(limit) instead of
          # holding every body in the stream at once.
          values.shift if values.size > limit
        end
      end
      values
    end

    def preload(*keys, max_messages: 10_000)
      @cache_mutex.synchronize { @cache ||= {} }
      keys.each do |key|
        # Only the last message decides the cache entry; stream the
        # scan so a long history never sits in memory all at once.
        last = nil
        count = consume_stream(queue_name(key), offset: "first", max_messages: max_messages) do |msg|
          last = msg
        end
        entry = last ? msg_to_cache_entry(last) : CacheEntry.new(value: nil, expires_at: nil)
        @cache_mutex.synchronize { @cache[key] = entry }
        log(:debug) { "at=preload key=#{key} messages=#{count} value=#{entry.current_value.nil? ? "nil" : "present"}" }
        start_cache_watcher(key)
      end
    end

    # Like `preload`, but reads from `x-stream-offset: last` so only the
    # current value is consumed (plus anything appended during the brief
    # `read_timeout` window). Use this when keys live in streams with long
    # histories and callers only care about the latest value — `preload`'s
    # `offset: "first"` would otherwise drain up to `max_messages` of
    # accumulated history per key on every boot.
    def preload_latest(*keys)
      @cache_mutex.synchronize { @cache ||= {} }
      keys.each do |key|
        messages = consume_stream(queue_name(key), offset: "last")
        entry = resolve_entry(messages)
        @cache_mutex.synchronize { @cache[key] = entry }
        log(:debug) { "at=preload_latest key=#{key} value=#{entry.current_value.nil? ? "nil" : "present"}" }
        start_cache_watcher(key)
      end
    end

    def watch(key, &block)
      name = queue_name(key)
      ensure_stream(name)
      ch = connection.channel
      ch.basic_qos(256)
      consume_ok = ch.basic_consume(name, no_ack: false,
                                    arguments: { "x-stream-offset" => "next" },
                                    worker_threads: 1) do |msg|
        msg.ack
        block.call(msg.body) unless tombstone?(msg)
      end
      WatchHandle.new(channel: ch, consumer_tag: consume_ok.consumer_tag)
    end

    def unwatch(handle)
      close_consumer(handle)
    end

    def purge!
      stop_cache_watchers
      conn = @mutex.synchronize { @connection }
      return unless conn && !conn.closed?

      streams = @mutex.synchronize { @declared_streams.to_a }
      conn.with_channel do |ch|
        streams.each { |name| ch.queue_delete(name) }
      end
      @mutex.synchronize { @declared_streams.clear }
    end

    def close
      stop_cache_watchers
      super
    end

    private

    def start_cache_watcher(key)
      return unless @cache_watchers_enabled

      @cache_mutex.synchronize { return if @cache_watchers.key?(key) }

      name = queue_name(key)
      ensure_stream(name)
      ch = connection.channel
      ch.basic_qos(256)
      consume_ok = ch.basic_consume(name, no_ack: false,
                                    arguments: { "x-stream-offset" => "next" },
                                    worker_threads: 1) do |msg|
        msg.ack
        entry = msg_to_cache_entry(msg)
        @cache_mutex.synchronize { @cache[key] = entry }
        log(:debug) { "at=cache_watcher key=#{key} value=#{entry.current_value.nil? ? "nil" : "present"}" }
      end
      handle = WatchHandle.new(channel: ch, consumer_tag: consume_ok.consumer_tag)

      duplicate = @cache_mutex.synchronize do
        if @cache_watchers.key?(key)
          true
        else
          @cache_watchers[key] = handle
          false
        end
      end

      if duplicate
        unwatch(handle)
      else
        log(:debug) { "at=cache_watcher key=#{key} status=started" }
      end
    end

    def stop_cache_watchers
      watchers = @cache_mutex.synchronize do
        result = @cache_watchers.values
        @cache_watchers.clear
        @cache = nil
        result
      end
      log(:debug) { "at=stop_cache_watchers count=#{watchers.size}" } if watchers.any?
      watchers.each do |handle|
        unwatch(handle)
      rescue StandardError
        nil
      end
    end
  end
end
