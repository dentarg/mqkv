#!/usr/bin/env ruby
# frozen_string_literal: true

# Usage: ruby examples/benchmark.rb [amqp_url]
#
# Benchmarks MQKV set/get operations against a running AMQP broker.

require "bundler/setup"
require "mqkv"
require "benchmark"

URL = ARGV[0] || ENV.fetch("AMQP_URL", "amqp://localhost")
N = (ENV["N"] || 50).to_i

def bench_ops(label, n)
  keys = (1..n).map { |i| "key-#{i}" }
  values = keys.map { |k| "value-for-#{k}" }

  set_time = Benchmark.realtime { keys.each_with_index { |k, i| yield :set, k, values[i] } }
  get_time = Benchmark.realtime { keys.each { |k| yield :get, k, nil } }

  printf "  %-20s  set: %7.1f ops/s (%5.1f ms/op)    get: %9.1f ops/s (%7.3f ms/op)\n",
         label,
         n / set_time, set_time / n * 1000,
         n / get_time, get_time / n * 1000
end

puts "MQKV Benchmark (#{N} keys, #{URL})"

# --- 1. Read timeout impact ---
puts
puts "--- read_timeout impact on GET (uncached) ---"

[0.5, 0.2, 0.1, 0.05].each do |timeout|
  store = MQKV::Store.new(URL, prefix: "mqkv-bench-#{Process.pid}-#{(timeout * 1000).to_i}",
                          read_timeout: timeout)
  store.set("warmup", "warmup")
  store.get("warmup")
  bench_ops("timeout=#{timeout}s", N) do |op, k, v|
    op == :set ? store.set(k, v) : store.get(k)
  end
  store.purge!
  store.close
end

# --- 2. Confirm vs no-confirm ---
puts
puts "--- confirm vs no-confirm SET ---"

[true, false].each do |confirm|
  store = MQKV::Store.new(URL, prefix: "mqkv-bench-confirm-#{Process.pid}-#{confirm}",
                          read_timeout: 0.05, confirm: confirm)
  store.set("warmup", "warmup")
  store.get("warmup")
  label = confirm ? "confirm: true" : "confirm: false"
  bench_ops(label, N) do |op, k, v|
    op == :set ? store.set(k, v) : store.get(k)
  end
  store.purge!
  store.close
end

# --- 3. Preload / cached GET ---
puts
puts "--- preload (cached GET) ---"

store = MQKV::Store.new(URL, prefix: "mqkv-bench-cache-#{Process.pid}", read_timeout: 0.05)
keys = (1..N).map { |i| "key-#{i}" }
keys.each_with_index { |k, i| store.set(k, "value-#{i}") }

# Uncached baseline
get_time = Benchmark.realtime { keys.each { |k| store.get(k) } }
printf "  %-20s  get: %9.1f ops/s (%7.3f ms/op)\n",
       "uncached", N / get_time, get_time / N * 1000

# Preload
preload_time = Benchmark.realtime { store.preload(*keys) }
printf "  %-20s  preload %d keys in %.1f ms\n", "preload", N, preload_time * 1000

# Cached
get_time = Benchmark.realtime { keys.each { |k| store.get(k) } }
printf "  %-20s  get: %9.1f ops/s (%7.3f ms/op)\n",
       "cached", N / get_time, get_time / N * 1000

store.purge!
store.close

puts
puts "NOTE: Uncached GET latency >= read_timeout (stream drain wait)."
puts "      Cached GET is a hash lookup - orders of magnitude faster."
