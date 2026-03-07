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

  def make_msg(body, tombstone: false)
    headers = tombstone ? { "__mqkv_deleted__" => true } : nil
    double("Message", body: body, properties: double("Properties", headers: headers))
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
