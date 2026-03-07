#!/usr/bin/env ruby
# frozen_string_literal: true

# Usage: ruby examples/benchmark.rb [amqp_url]
#
# Benchmarks MQKV set/get operations against a running AMQP broker.
# GET latency is bounded by read_timeout (the stream drain wait).

require "bundler/setup"
require "mqkv"
require "benchmark"

URL = ARGV[0] || ENV.fetch("AMQP_URL", "amqp://localhost")
N = (ENV["N"] || 50).to_i

def run_bench(label, store, n)
  keys = (1..n).map { |i| "key-#{i}" }
  values = keys.map { |k| "value-for-#{k}" }

  set_time = Benchmark.realtime { keys.each_with_index { |k, i| store.set(k, values[i]) } }
  get_time = Benchmark.realtime { keys.each { |k| store.get(k) } }

  printf "  %-14s  set: %6.1f ops/s (%5.1f ms/op)    get: %5.1f ops/s (%5.1f ms/op)\n",
         label,
         n / set_time, set_time / n * 1000,
         n / get_time, get_time / n * 1000

  store.purge!
end

puts "MQKV Benchmark (#{N} keys, #{URL})"
puts
puts "--- read_timeout impact on GET ---"

[0.5, 0.2, 0.1, 0.05].each do |timeout|
  store = MQKV::Store.new(URL, prefix: "mqkv-bench-#{Process.pid}-#{(timeout * 1000).to_i}",
                          read_timeout: timeout)
  store.set("warmup", "warmup")
  store.get("warmup")
  run_bench("timeout=#{timeout}s", store, N)
  store.close
end

puts
puts "--- value size impact (read_timeout=0.05s) ---"

store = MQKV::Store.new(URL, prefix: "mqkv-bench-size-#{Process.pid}", read_timeout: 0.05)

[64, 1_000, 10_000].each do |size|
  value = "x" * size
  label = size >= 1000 ? "#{size / 1000}KB" : "#{size}B"

  set_time = Benchmark.realtime { N.times { store.set("sized", value) } }
  get_time = Benchmark.realtime { N.times { store.get("sized") } }

  printf "  %-14s  set: %6.1f ops/s (%5.1f ms/op)    get: %5.1f ops/s (%5.1f ms/op)\n",
         label,
         N / set_time, set_time / N * 1000,
         N / get_time, get_time / N * 1000
end

store.purge!
store.close
puts
puts "NOTE: GET latency >= read_timeout (stream drain wait). Lower timeout = faster"
puts "reads, but risks missing the last message on slow brokers."
