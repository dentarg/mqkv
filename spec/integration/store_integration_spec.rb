# frozen_string_literal: true

require "securerandom"

RSpec.describe MQKV::Store, :integration do
  before(:all) do
    skip "Set AMQP_URL to run integration tests" unless ENV["AMQP_URL"]
  end

  let(:url) { ENV.fetch("AMQP_URL") }
  let(:prefix) { "mqkv-test-#{SecureRandom.hex(4)}" }
  let(:store) { described_class.new(url, prefix: prefix, read_timeout: 0.3) }

  after do
    store.purge!
    store.close
  end

  describe "set and get" do
    it "stores and retrieves a value" do
      store.set("k1", "hello")
      expect(store.get("k1")).to eq("hello")
    end

    it "overwrites a value" do
      store.set("k2", "first")
      store.set("k2", "second")
      expect(store.get("k2")).to eq("second")
    end

    it "returns nil for a non-existent key" do
      expect(store.get("missing")).to be_nil
    end
  end

  describe "delete and exists?" do
    it "deletes a key" do
      store.set("k3", "world")
      store.delete("k3")
      expect(store.get("k3")).to be_nil
    end

    it "checks existence" do
      expect(store.exists?("k4")).to be false
      store.set("k4", "yes")
      expect(store.exists?("k4")).to be true
      store.delete("k4")
      expect(store.exists?("k4")).to be false
    end
  end

  describe "#history" do
    it "returns value history" do
      store.set("h1", "a")
      store.set("h1", "b")
      store.set("h1", "c")
      expect(store.history("h1")).to eq(%w[a b c])
    end

    it "clears history on delete" do
      store.set("h2", "x")
      store.set("h2", "y")
      store.delete("h2")
      store.set("h2", "z")
      expect(store.history("h2")).to eq(["z"])
    end

    it "respects limit" do
      store.set("h3", "1")
      store.set("h3", "2")
      store.set("h3", "3")
      expect(store.history("h3", limit: 2)).to eq(%w[2 3])
    end
  end

  describe "watch and unwatch" do
    it "receives new values via watch" do
      received = []
      mutex = Mutex.new
      handle = store.watch("w1") { |val| mutex.synchronize { received << val } }
      sleep 0.2
      store.set("w1", "first")
      store.set("w1", "second")
      sleep 0.5
      store.unwatch(handle)
      expect(received).to eq(%w[first second])
    end
  end
end
