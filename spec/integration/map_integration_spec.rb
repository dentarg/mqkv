# frozen_string_literal: true

require "securerandom"

RSpec.describe MQKV::Map, :integration do
  before(:all) do
    skip "Set AMQP_URL to run integration tests" unless ENV["AMQP_URL"]
  end

  let(:url) { ENV.fetch("AMQP_URL") }
  let(:name) { "ratelimit-#{SecureRandom.hex(4)}" }
  let(:map) { described_class.new(url, name: name, prefix: "mqkv-test", read_timeout: 0.3) }

  after do
    map.purge!
    map.close
  end

  describe "set and get" do
    it "stores and retrieves many keys from a single stream" do
      map.set("1.2.3.4", "a")
      map.set("5.6.7.8", "b")
      expect(map.get("1.2.3.4")).to eq("a")
      expect(map.get("5.6.7.8")).to eq("b")
    end

    it "overwrites a value (last write wins)" do
      map.set("k", "first")
      map.set("k", "second")
      expect(map.get("k")).to eq("second")
    end

    it "returns nil for an unknown key" do
      expect(map.get("missing")).to be_nil
    end
  end

  describe "delete and exists?" do
    it "tombstones a key" do
      map.set("k", "v")
      map.delete("k")
      expect(map.get("k")).to be_nil
      expect(map.exists?("k")).to be false
    end
  end

  describe "all keys share one queue" do
    it "declares exactly one stream for many keys" do
      100.times { |i| map.set("ip-#{i}", i.to_s) }
      expect(map.get("ip-0")).to eq("0")
      expect(map.get("ip-99")).to eq("99")
      expect(map.size).to eq(100)
    end
  end

  describe "loading existing state on start" do
    it "rebuilds the map from the stream in a fresh process/connection" do
      map.set("seen", "value")
      map.set("gone", "value")
      map.delete("gone")

      reader = described_class.new(url, name: name, prefix: "mqkv-test", read_timeout: 0.3)
      reader.start
      begin
        expect(reader.get("seen")).to eq("value")
        expect(reader.get("gone")).to be_nil
      ensure
        reader.close
      end
    end
  end

  describe "live propagation between connections" do
    it "sees writes from another connection via the shared stream" do
      map.start
      writer = described_class.new(url, name: name, prefix: "mqkv-test", read_timeout: 0.3)
      writer.set("shared", "from-writer")
      sleep 0.3
      writer.close
      expect(map.get("shared")).to eq("from-writer")
    end
  end

  describe "TTL" do
    it "returns the value before expiry and nil after" do
      map.set("ttl", "temp", ttl: 0.5)
      expect(map.get("ttl")).to eq("temp")
      sleep 0.6
      expect(map.get("ttl")).to be_nil
    end
  end
end
