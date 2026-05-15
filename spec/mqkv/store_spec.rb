# frozen_string_literal: true

RSpec.describe MQKV::Store do
  let(:url) { "amqp://localhost" }
  let(:store) { described_class.new(url, prefix: "test") }
  let(:conn) { instance_double(AMQP::Client::Connection) }
  let(:channel) { instance_double(AMQP::Client::Connection::Channel) }

  before do
    amqp = instance_double(AMQP::Client)
    allow(AMQP::Client).to receive(:new).with(url).and_return(amqp)
    allow(amqp).to receive(:connect).and_return(conn)
    allow(conn).to receive(:closed?).and_return(false)
    allow(conn).to receive(:close)
    allow(conn).to receive(:channel).and_return(channel)
    allow(conn).to receive(:with_channel).and_yield(channel)
    allow(channel).to receive(:close)
    allow(channel).to receive(:queue_declare)
  end

  def make_msg(body, tombstone: false, expires_at: nil)
    headers = {}
    headers["__mqkv_deleted__"] = true if tombstone
    headers["__mqkv_expires_at__"] = expires_at if expires_at
    double("Message", body: body, properties: double("Properties", headers: headers.empty? ? nil : headers))
  end

  describe "queue naming" do
    it "combines prefix and key" do
      expect(store.send(:queue_name, "user:1")).to eq("test.user:1")
    end
  end

  describe "tombstone detection" do
    it "detects tombstone messages" do
      expect(store.send(:tombstone?, make_msg("", tombstone: true))).to be true
    end

    it "returns false for regular messages" do
      expect(store.send(:tombstone?, make_msg("hello"))).to be false
    end

    it "handles nil properties" do
      msg = double("Message", properties: nil)
      expect(store.send(:tombstone?, msg)).to be false
    end
  end

  describe "#set" do
    before do
      allow(channel).to receive(:basic_publish_confirm).and_return(true)
    end

    it "publishes to the correct queue with confirmed delivery" do
      store.set("mykey", "myvalue")
      expect(channel).to have_received(:basic_publish_confirm)
        .with("myvalue", exchange: "", routing_key: "test.mykey")
    end

    it "converts value to string" do
      store.set("num", 42)
      expect(channel).to have_received(:basic_publish_confirm)
        .with("42", exchange: "", routing_key: "test.num")
    end

    it "publishes with expires_at header and declares with x-max-age when ttl given" do
      store.set("mykey", "myvalue", ttl: 60)
      expect(channel).to have_received(:basic_publish_confirm)
        .with("myvalue", exchange: "", routing_key: "test.mykey",
              headers: hash_including("__mqkv_expires_at__" => a_kind_of(Numeric)))
      expect(channel).to have_received(:queue_declare)
        .with("test.mykey", durable: true,
              arguments: { "x-queue-type" => "stream", "x-max-age" => "60s" })
    end
  end

  describe "#set with confirm: false" do
    let(:store) { described_class.new(url, prefix: "test", confirm: false) }

    it "uses basic_publish without confirmation" do
      allow(channel).to receive(:basic_publish)
      store.set("mykey", "myvalue")
      expect(channel).to have_received(:basic_publish)
        .with("myvalue", exchange: "", routing_key: "test.mykey")
    end
  end

  describe "#get" do
    it "returns nil when no messages" do
      allow(store).to receive(:consume_stream).and_return([])
      expect(store.get("key")).to be_nil
    end

    it "returns the body of the last message" do
      allow(store).to receive(:consume_stream)
        .with("test.key", offset: "last")
        .and_return([make_msg("hello")])
      expect(store.get("key")).to eq("hello")
    end

    it "returns nil for tombstone" do
      allow(store).to receive(:consume_stream)
        .and_return([make_msg("", tombstone: true)])
      expect(store.get("key")).to be_nil
    end

    it "returns nil for expired message" do
      expired = make_msg("old", expires_at: Process.clock_gettime(Process::CLOCK_REALTIME) - 1)
      allow(store).to receive(:consume_stream).and_return([expired])
      expect(store.get("key")).to be_nil
    end

    it "returns body for non-expired message" do
      future = make_msg("fresh", expires_at: Process.clock_gettime(Process::CLOCK_REALTIME) + 3600)
      allow(store).to receive(:consume_stream).and_return([future])
      expect(store.get("key")).to eq("fresh")
    end

    it "uses the last message when multiple are returned" do
      msgs = [make_msg("old"), make_msg("new")]
      allow(store).to receive(:consume_stream).and_return(msgs)
      expect(store.get("key")).to eq("new")
    end
  end

  describe "#delete" do
    it "publishes a tombstone message" do
      allow(channel).to receive(:basic_publish_confirm).and_return(true)
      store.delete("mykey")
      expect(channel).to have_received(:basic_publish_confirm)
        .with("", exchange: "", routing_key: "test.mykey",
              headers: { "__mqkv_deleted__" => true })
    end
  end

  describe "#exists?" do
    it "returns true when get returns a value" do
      allow(store).to receive(:get).with("key").and_return("value")
      expect(store.exists?("key")).to be true
    end

    it "returns false when get returns nil" do
      allow(store).to receive(:get).with("key").and_return(nil)
      expect(store.exists?("key")).to be false
    end
  end

  describe "#history" do
    it "returns all values in order" do
      msgs = [make_msg("a"), make_msg("b"), make_msg("c")]
      allow(store).to receive(:consume_stream)
        .with("test.key", offset: "first").and_return(msgs)
      expect(store.history("key")).to eq(%w[a b c])
    end

    it "clears accumulated values on tombstone" do
      msgs = [make_msg("a"), make_msg("b"),
              make_msg("", tombstone: true),
              make_msg("c")]
      allow(store).to receive(:consume_stream).and_return(msgs)
      expect(store.history("key")).to eq(["c"])
    end

    it "respects limit" do
      msgs = (1..5).map { |i| make_msg(i.to_s) }
      allow(store).to receive(:consume_stream).and_return(msgs)
      expect(store.history("key", limit: 3)).to eq(%w[3 4 5])
    end

    it "returns empty array when no messages" do
      allow(store).to receive(:consume_stream).and_return([])
      expect(store.history("key")).to eq([])
    end
  end

  describe "#preload" do
    it "populates cache from stream messages" do
      msgs = [make_msg("a"), make_msg("b")]
      allow(store).to receive(:consume_stream)
        .with("test.key", offset: "first", max_messages: 10_000)
        .and_return(msgs)
      allow(store).to receive(:start_cache_watcher)
      store.preload("key")
      expect(store.get("key")).to eq("b")
    end

    it "resolves tombstones during preload" do
      msgs = [make_msg("a"), make_msg("", tombstone: true)]
      allow(store).to receive(:consume_stream).and_return(msgs)
      allow(store).to receive(:start_cache_watcher)
      store.preload("key")
      expect(store.get("key")).to be_nil
    end

    it "returns cached value without calling consume_stream" do
      allow(store).to receive(:consume_stream).and_return([make_msg("cached")])
      allow(store).to receive(:start_cache_watcher)
      store.preload("key")

      allow(store).to receive(:consume_stream).and_raise("should not be called")
      expect(store.get("key")).to eq("cached")
    end

    it "falls back to stream for non-cached keys" do
      allow(store).to receive(:consume_stream).and_return([])
      allow(store).to receive(:start_cache_watcher)
      store.preload("cached")

      allow(store).to receive(:consume_stream)
        .with("test.other", offset: "last")
        .and_return([make_msg("from-stream")])
      expect(store.get("other")).to eq("from-stream")
    end
  end

  describe "#preload_latest" do
    it "consumes from offset=last and caches the latest value" do
      allow(store).to receive(:consume_stream)
        .with("test.key", offset: "last")
        .and_return([make_msg("current")])
      allow(store).to receive(:start_cache_watcher)
      store.preload_latest("key")
      expect(store.get("key")).to eq("current")
    end

    it "caches nil when the latest message is a tombstone" do
      allow(store).to receive(:consume_stream)
        .with("test.key", offset: "last")
        .and_return([make_msg("", tombstone: true)])
      allow(store).to receive(:start_cache_watcher)
      store.preload_latest("key")
      expect(store.get("key")).to be_nil
    end

    it "caches nil for an empty stream" do
      allow(store).to receive(:consume_stream)
        .with("test.key", offset: "last")
        .and_return([])
      allow(store).to receive(:start_cache_watcher)
      store.preload_latest("key")
      expect(store.get("key")).to be_nil
    end

    it "subsequent reads come from the cache without consuming the stream" do
      allow(store).to receive(:consume_stream)
        .with("test.key", offset: "last")
        .and_return([make_msg("cached")])
      allow(store).to receive(:start_cache_watcher)
      store.preload_latest("key")

      allow(store).to receive(:consume_stream).and_raise("should not be called")
      expect(store.get("key")).to eq("cached")
    end
  end

  describe "cache updates via set/delete" do
    before do
      allow(store).to receive(:consume_stream).and_return([])
      allow(store).to receive(:start_cache_watcher)
      store.preload("key")
      allow(channel).to receive(:basic_publish_confirm).and_return(true)
    end

    it "updates cache on set" do
      store.set("key", "new-value")
      expect(store.get("key")).to eq("new-value")
    end

    it "sets cache to nil on delete" do
      store.set("key", "value")
      store.delete("key")
      expect(store.get("key")).to be_nil
    end
  end

  describe "TTL in cache" do
    before do
      allow(store).to receive(:consume_stream).and_return([])
      allow(store).to receive(:start_cache_watcher)
      store.preload("key")
      allow(channel).to receive(:basic_publish_confirm).and_return(true)
    end

    it "returns value before expiry" do
      store.set("key", "temp", ttl: 3600)
      expect(store.get("key")).to eq("temp")
    end

    it "returns nil after expiry" do
      store.set("key", "temp", ttl: 0)
      sleep 0.01
      expect(store.get("key")).to be_nil
    end
  end

  describe "CacheEntry" do
    it "returns value when not expired" do
      entry = described_class::CacheEntry.new(value: "hello", expires_at: nil)
      expect(entry.current_value).to eq("hello")
    end

    it "returns nil for tombstone entry" do
      entry = described_class::CacheEntry.new(value: nil, expires_at: nil)
      expect(entry.current_value).to be_nil
    end

    it "returns nil when expired" do
      entry = described_class::CacheEntry.new(
        value: "old",
        expires_at: Process.clock_gettime(Process::CLOCK_REALTIME) - 1
      )
      expect(entry.current_value).to be_nil
    end

    it "returns value when not yet expired" do
      entry = described_class::CacheEntry.new(
        value: "fresh",
        expires_at: Process.clock_gettime(Process::CLOCK_REALTIME) + 3600
      )
      expect(entry.current_value).to eq("fresh")
    end
  end

  describe "logging" do
    let(:log_output) { StringIO.new }
    let(:logger) { Logger.new(log_output, level: :debug) }
    let(:store) { described_class.new(url, prefix: "test", logger: logger) }

    it "logs set, get, and connect when logger is provided" do
      allow(channel).to receive(:basic_publish_confirm).and_return(true)
      allow(store).to receive(:consume_stream).and_return([make_msg("val")])

      store.set("k", "v")
      store.get("k")
      store.close

      output = log_output.string
      expect(output).to include("at=connect")
      expect(output).to include("at=set key=k")
      expect(output).to include("at=get key=k source=stream")
      expect(output).to include("at=close")
    end

    it "does not log credentials from the AMQP URL" do
      cred_url = "amqp://admin:s3cret@rabbit.example.com:5672/prod"
      amqp = instance_double(AMQP::Client)
      allow(AMQP::Client).to receive(:new).with(cred_url).and_return(amqp)
      allow(amqp).to receive(:connect).and_return(conn)

      cred_store = described_class.new(cred_url, prefix: "test", logger: logger)
      allow(channel).to receive(:basic_publish_confirm).and_return(true)
      cred_store.set("k", "v")

      output = log_output.string
      expect(output).to include("at=connect")
      expect(output).to include("rabbit.example.com")
      expect(output).not_to include("admin")
      expect(output).not_to include("s3cret")
    end
  end

  describe "stream declaration caching" do
    it "only declares once per queue name" do
      allow(channel).to receive(:basic_publish_confirm).and_return(true)
      store.set("key", "a")
      store.set("key", "b")
      expect(channel).to have_received(:queue_declare)
        .with("test.key", durable: true, arguments: { "x-queue-type" => "stream" })
        .once
    end

    it "declares different queues independently" do
      allow(channel).to receive(:basic_publish_confirm).and_return(true)
      store.set("key1", "a")
      store.set("key2", "b")
      expect(channel).to have_received(:queue_declare).twice
    end
  end
end
