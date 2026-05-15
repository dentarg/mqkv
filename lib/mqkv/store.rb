# frozen_string_literal: true

require "amqp-client"
require "logger"
require "set"

module MQKV
  class Store
    WatchHandle = Data.define(:channel, :consumer_tag)

    CacheEntry = Data.define(:value, :expires_at) do
      def current_value
        return nil if value.nil?
        return nil if expires_at && Process.clock_gettime(Process::CLOCK_REALTIME) >= expires_at

        value
      end
    end

    TOMBSTONE_HEADER = "__mqkv_deleted__"
    EXPIRES_HEADER = "__mqkv_expires_at__"

    def initialize(url, prefix: "mqkv", read_timeout: 0.5, confirm: true, logger: nil)
      @url = url
      @prefix = prefix
      @read_timeout = read_timeout
      @confirm = confirm
      @logger = logger
      @mutex = Mutex.new
      @connection = nil
      @declared_streams = Set.new
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
      messages = consume_stream(queue_name(key), offset: "first")
      values = []
      messages.each do |msg|
        if tombstone?(msg)
          values.clear
        else
          values << msg.body
        end
      end
      values.last(limit)
    end

    def preload(*keys, max_messages: 10_000)
      @cache_mutex.synchronize { @cache ||= {} }
      keys.each do |key|
        messages = consume_stream(queue_name(key), offset: "first", max_messages: max_messages)
        entry = resolve_entry(messages)
        @cache_mutex.synchronize { @cache[key] = entry }
        log(:debug) { "at=preload key=#{key} messages=#{messages.size} value=#{entry.current_value.nil? ? "nil" : "present"}" }
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
      handle.channel.basic_cancel(handle.consumer_tag)
      handle.channel.close
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
      log(:info) { "at=close" }
      stop_cache_watchers
      @mutex.synchronize do
        @connection&.close
        @connection = nil
        @declared_streams.clear
      end
    end

    private

    def connection
      @mutex.synchronize do
        return @connection if @connection && !@connection.closed?

        log(:info) { "at=connect url=#{sanitized_url}" }
        @connection = AMQP::Client.new(@url).connect
        @declared_streams.clear
        @connection
      end
    end

    def queue_name(key)
      "#{@prefix}.#{key}"
    end

    def ensure_stream(name, max_age: nil)
      already_declared = @mutex.synchronize { @declared_streams.include?(name) }
      return if already_declared

      args = { "x-queue-type" => "stream" }
      args["x-max-age"] = max_age if max_age

      connection.with_channel do |ch|
        ch.queue_declare(name, durable: true, arguments: args)
      end
      @mutex.synchronize { @declared_streams.add(name) }
      log(:debug) { "at=ensure_stream queue=#{name} status=declared" }
    end

    def publish(name, body, **properties)
      properties.compact!
      connection.with_channel do |ch|
        if @confirm
          ch.basic_publish_confirm(body, exchange: "", routing_key: name, **properties)
        else
          ch.basic_publish(body, exchange: "", routing_key: name, **properties)
        end
      end
    end

    def publish_tombstone(name)
      publish(name, "", headers: { TOMBSTONE_HEADER => true })
    end

    def consume_stream(name, offset:, max_messages: 0)
      ensure_stream(name)
      ch = connection.channel
      begin
        ch.basic_qos(256)
        collected = []
        q = ::Queue.new

        consume_ok = ch.basic_consume(name, no_ack: false,
                                      arguments: { "x-stream-offset" => offset },
                                      worker_threads: 1) do |msg|
          q.push(msg)
        end

        loop do
          msg = q.pop(timeout: @read_timeout)
          break if msg.nil?

          collected << msg
          break if max_messages > 0 && collected.size >= max_messages
        end

        ch.basic_ack(collected.last.delivery_tag, multiple: true) if collected.any?
        ch.basic_cancel(consume_ok.consumer_tag)
        collected
      ensure
        ch.close
      end
    end

    def resolve_entry(messages)
      return CacheEntry.new(value: nil, expires_at: nil) if messages.empty?

      last = messages.last
      return CacheEntry.new(value: nil, expires_at: nil) if tombstone?(last)

      CacheEntry.new(value: last.body, expires_at: msg_expires_at(last))
    end

    def resolve_current(messages)
      resolve_entry(messages).current_value
    end

    def tombstone?(msg)
      msg.properties&.headers&.fetch(TOMBSTONE_HEADER, false) == true
    end

    def msg_expires_at(msg)
      msg.properties&.headers&.fetch(EXPIRES_HEADER, nil)
    end

    def msg_to_cache_entry(msg)
      if tombstone?(msg)
        CacheEntry.new(value: nil, expires_at: nil)
      else
        CacheEntry.new(value: msg.body, expires_at: msg_expires_at(msg))
      end
    end

    def start_cache_watcher(key)
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

    def sanitized_url
      @url.sub(%r{//[^@]+@}, "//")
    end

    def log(level, &block)
      @logger&.send(level, &block)
    end
  end
end
