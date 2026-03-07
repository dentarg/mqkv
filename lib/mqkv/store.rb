# frozen_string_literal: true

require "amqp-client"
require "set"

module MQKV
  class Store
    WatchHandle = Data.define(:channel, :consumer_tag)

    def initialize(url, prefix: "mqkv", read_timeout: 0.5)
      @url = url
      @prefix = prefix
      @read_timeout = read_timeout
      @mutex = Mutex.new
      @connection = nil
      @declared_streams = Set.new
    end

    def set(key, value)
      name = queue_name(key)
      ensure_stream(name)
      connection.with_channel do |ch|
        ch.basic_publish_confirm(value.to_s, exchange: "", routing_key: name)
      end
      nil
    end

    def get(key)
      messages = consume_stream(queue_name(key), offset: "last")
      return nil if messages.empty?

      last = messages.last
      return nil if tombstone?(last)

      last.body
    end

    def delete(key)
      name = queue_name(key)
      ensure_stream(name)
      connection.with_channel do |ch|
        ch.basic_publish_confirm("", exchange: "", routing_key: name,
                                 headers: { "__mqkv_deleted__" => true })
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
      conn = @mutex.synchronize { @connection }
      return unless conn && !conn.closed?

      streams = @mutex.synchronize { @declared_streams.to_a }
      conn.with_channel do |ch|
        streams.each { |name| ch.queue_delete(name) }
      end
      @mutex.synchronize { @declared_streams.clear }
    end

    def close
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

        @connection = AMQP::Client.new(@url).connect
        @declared_streams.clear
        @connection
      end
    end

    def queue_name(key)
      "#{@prefix}.#{key}"
    end

    def ensure_stream(name)
      already_declared = @mutex.synchronize { @declared_streams.include?(name) }
      return if already_declared

      connection.with_channel do |ch|
        ch.queue_declare(name, durable: true, arguments: { "x-queue-type" => "stream" })
      end
      @mutex.synchronize { @declared_streams.add(name) }
    end

    def consume_stream(name, offset:)
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
        end

        ch.basic_ack(collected.last.delivery_tag, multiple: true) if collected.any?
        ch.basic_cancel(consume_ok.consumer_tag)
        collected
      ensure
        ch.close
      end
    end

    def tombstone?(msg)
      msg.properties&.headers&.fetch("__mqkv_deleted__", false) == true
    end
  end
end
