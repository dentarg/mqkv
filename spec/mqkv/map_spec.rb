# frozen_string_literal: true

RSpec.describe MQKV::Map do
  let(:url) { "amqp://localhost" }
  let(:map) { described_class.new(url, name: "ratelimit", prefix: "test") }
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
    allow(channel).to receive(:basic_publish_confirm).and_return(true)
    allow(channel).to receive(:basic_publish)
    # Most specs drive the in-memory map directly; skip the network-bound
    # start so get/set/delete don't try to consume the stream.
    map.instance_variable_set(:@started, true)
    allow(map).to receive(:start).and_return(map)
  end

  def make_msg(body, key: nil, tombstone: false, expires_at: nil, sync: nil)
    headers = {}
    headers["__mqkv_key__"] = key if key
    headers["__mqkv_deleted__"] = true if tombstone
    headers["__mqkv_expires_at__"] = expires_at if expires_at
    headers["__mqkv_sync__"] = sync if sync
    double("Message", body: body, ack: nil,
                      properties: double("Properties", headers: headers.empty? ? nil : headers))
  end

  describe "stream naming" do
    it "uses a single queue from prefix and name" do
      expect(map.send(:stream_name)).to eq("test.ratelimit")
    end
  end

  describe "#set" do
    it "publishes to the single stream tagged with the key header" do
      map.set("1.2.3.4", "v")
      expect(channel).to have_received(:basic_publish_confirm)
        .with("v", exchange: "", routing_key: "test.ratelimit",
              headers: { "__mqkv_key__" => "1.2.3.4" })
    end

    it "converts the value to a string" do
      map.set("k", 42)
      expect(channel).to have_received(:basic_publish_confirm)
        .with("42", exchange: "", routing_key: "test.ratelimit",
              headers: { "__mqkv_key__" => "k" })
    end

    it "reads back its own write from memory without consuming the stream" do
      map.set("k", "v")
      expect(map.get("k")).to eq("v")
    end

    it "includes an expires header when given a ttl" do
      map.set("k", "v", ttl: 60)
      expect(channel).to have_received(:basic_publish_confirm)
        .with("v", exchange: "", routing_key: "test.ratelimit",
              headers: hash_including("__mqkv_key__" => "k",
                                      "__mqkv_expires_at__" => a_kind_of(Numeric)))
    end

    it "returns nil once the ttl has passed" do
      map.set("k", "v", ttl: 0)
      sleep 0.01
      expect(map.get("k")).to be_nil
    end
  end

  describe "#set with confirm: false" do
    let(:map) { described_class.new(url, name: "ratelimit", prefix: "test", confirm: false) }

    it "publishes without confirmation" do
      map.set("k", "v")
      expect(channel).to have_received(:basic_publish)
        .with("v", exchange: "", routing_key: "test.ratelimit",
              headers: { "__mqkv_key__" => "k" })
    end
  end

  describe "#delete" do
    it "publishes a tombstone tagged with the key header and drops it from memory" do
      map.set("k", "v")
      map.delete("k")
      expect(channel).to have_received(:basic_publish_confirm)
        .with("", exchange: "", routing_key: "test.ratelimit",
              headers: { "__mqkv_deleted__" => true, "__mqkv_key__" => "k" })
      expect(map.get("k")).to be_nil
    end
  end

  describe "#get" do
    it "returns nil for an unknown key" do
      expect(map.get("missing")).to be_nil
    end
  end

  describe "#exists?" do
    it "is true for a live key and false otherwise" do
      map.set("k", "v")
      expect(map.exists?("k")).to be true
      expect(map.exists?("nope")).to be false
    end
  end

  describe "applying delivered messages" do
    it "stores a value message under its key" do
      map.send(:apply, make_msg("v", key: "k"))
      expect(map.get("k")).to eq("v")
    end

    it "applies messages last-write-wins in delivery order" do
      map.send(:apply, make_msg("old", key: "k"))
      map.send(:apply, make_msg("new", key: "k"))
      expect(map.get("k")).to eq("new")
    end

    it "removes a key on tombstone" do
      map.send(:apply, make_msg("v", key: "k"))
      map.send(:apply, make_msg("", key: "k", tombstone: true))
      expect(map.get("k")).to be_nil
      expect(map.size).to eq(0)
    end

    it "drops already-expired messages instead of storing them" do
      past = Process.clock_gettime(Process::CLOCK_REALTIME) - 1
      map.send(:apply, make_msg("old", key: "k", expires_at: past))
      expect(map.size).to eq(0)
    end

    it "ignores messages without a key header" do
      map.send(:apply, make_msg("orphan"))
      expect(map.size).to eq(0)
    end
  end

  describe "#size" do
    it "counts only live (non-expired) keys" do
      map.set("a", "1")
      map.set("b", "2", ttl: 0)
      sleep 0.01
      expect(map.size).to eq(1)
    end
  end

  describe "#sweep" do
    it "drops expired entries from memory" do
      map.set("a", "1")
      map.set("b", "2", ttl: 0)
      sleep 0.01
      expect(map.instance_variable_get(:@map).size).to eq(2)
      map.send(:sweep)
      expect(map.instance_variable_get(:@map).keys).to eq(["a"])
    end
  end

  describe "max_age normalization" do
    it "converts a numeric to a seconds string" do
      expect(map.send(:normalize_max_age, 3600)).to eq("3600s")
    end

    it "passes a string through unchanged" do
      expect(map.send(:normalize_max_age, "1h")).to eq("1h")
    end

    it "is nil when unset" do
      expect(map.send(:normalize_max_age, nil)).to be_nil
    end
  end

  describe "#start" do
    let(:map) { described_class.new(url, name: "ratelimit", prefix: "test", read_timeout: 0.05, max_age: 3600) }
    let(:consume_ok) { double("ConsumeOk", consumer_tag: "tag-1") }

    before do
      map.instance_variable_set(:@started, false)
      allow(map).to receive(:start).and_call_original
      allow(channel).to receive(:basic_qos)
      allow(channel).to receive(:basic_ack)
      allow(channel).to receive(:basic_cancel)

      # The marker is published before subscribing (to prime the stream),
      # so record its token on publish, then replay history + the marker
      # to the consumer block on subscribe — mimicking the broker fanning
      # the primed stream out so start detects "caught up".
      history = [make_msg("v1", key: "a"), make_msg("v2", key: "b")]
      published_sync = nil
      allow(channel).to receive(:basic_publish_confirm) do |_body, **props|
        token = props.dig(:headers, "__mqkv_sync__")
        published_sync = token if token
        true
      end
      allow(channel).to receive(:basic_consume) do |*_args, **_kwargs, &blk|
        history.each { |m| blk.call(m) }
        blk.call(make_msg("", sync: published_sync)) if published_sync
        consume_ok
      end
    end

    it "loads existing state from the single stream" do
      map.start
      expect(map.get("a")).to eq("v1")
      expect(map.get("b")).to eq("v2")
    end

    it "declares the stream once with the retention argument" do
      map.start
      expect(channel).to have_received(:queue_declare)
        .with("test.ratelimit", durable: true,
              arguments: { "x-queue-type" => "stream", "x-max-age" => "3600s" })
        .once
    end

    it "consumes the stream from the beginning" do
      map.start
      expect(channel).to have_received(:basic_consume)
        .with("test.ratelimit", no_ack: false,
              arguments: { "x-stream-offset" => "first" }, worker_threads: 1)
    end

    it "is idempotent" do
      map.start
      map.start
      expect(channel).to have_received(:basic_consume).once
    end
  end
end
