# frozen_string_literal: true

require "amqp-client"
require "logger"
require "set"

module MQKV
  # Shared AMQP plumbing for the mqkv store types: connection management,
  # stream declaration, publishing, and the batched-ack stream scanner.
  # `Store` (one queue per key) and `Map` (many keys per queue) both build
  # on this base.
  class Base
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

    # Stream consumers must ack to advance the broker's flow-control
    # window: with manual acks and a prefetch of N, delivery stalls
    # after N outstanding messages. Acking every half-window keeps
    # deliveries flowing while still batching (multiple: true).
    CONSUME_PREFETCH = 256
    CONSUME_ACK_BATCH = CONSUME_PREFETCH / 2

    def initialize(url, prefix:, read_timeout:, connect_timeout:, confirm:, logger:)
      @url = url
      @prefix = prefix
      @read_timeout = read_timeout
      @connect_timeout = connect_timeout
      @confirm = confirm
      @logger = logger
      @mutex = Mutex.new
      @connection = nil
      @declared_streams = Set.new
    end

    def close
      log(:info) { "at=close" }
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
        @connection = AMQP::Client.new(@url, **client_options).connect
        @declared_streams.clear
        @connection
      end
    end

    def client_options
      @connect_timeout ? { connect_timeout: @connect_timeout } : {}
    end

    def queue_name(key)
      "#{@prefix}.#{key}"
    end

    def ensure_stream(name, max_age: nil, max_length_bytes: nil)
      already_declared = @mutex.synchronize { @declared_streams.include?(name) }
      return if already_declared

      args = { "x-queue-type" => "stream" }
      args["x-max-age"] = max_age if max_age
      args["x-max-length-bytes"] = max_length_bytes if max_length_bytes

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

    def publish_tombstone(name, headers: nil)
      publish(name, "", headers: { TOMBSTONE_HEADER => true }.merge(headers || {}))
    end

    # Drains the stream from `offset` until it goes quiet for
    # `read_timeout`. With a block, each message is yielded as it
    # arrives and the message count is returned — nothing is retained,
    # so callers control their own memory. Without a block, all
    # messages are collected and returned (only suitable for short
    # reads like offset=last). Acks are issued in batches as the scan
    # progresses; a single ack at the end would stall delivery once
    # the prefetch window fills and silently truncate longer streams.
    def consume_stream(name, offset:, max_messages: 0)
      ensure_stream(name)
      ch = connection.channel
      begin
        ch.basic_qos(CONSUME_PREFETCH)
        collected = block_given? ? nil : []
        count = 0
        unacked = 0
        last = nil
        q = ::Queue.new

        consume_ok = ch.basic_consume(name, no_ack: false,
                                      arguments: { "x-stream-offset" => offset },
                                      worker_threads: 1) do |msg|
          q.push(msg)
        end

        loop do
          msg = q.pop(timeout: @read_timeout)
          break if msg.nil?

          count += 1
          last = msg
          if collected
            collected << msg
          else
            yield msg
          end

          unacked += 1
          if unacked >= CONSUME_ACK_BATCH
            ch.basic_ack(msg.delivery_tag, multiple: true)
            unacked = 0
          end
          break if max_messages > 0 && count >= max_messages
        end

        ch.basic_ack(last.delivery_tag, multiple: true) if last && unacked > 0
        ch.basic_cancel(consume_ok.consumer_tag)
        collected || count
      ensure
        ch.close
      end
    end

    def close_consumer(handle)
      handle.channel.basic_cancel(handle.consumer_tag)
      handle.channel.close
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

    def sanitized_url
      @url.sub(%r{//[^@]+@}, "//")
    end

    def log(level, &block)
      @logger&.send(level, &block)
    end
  end
end
