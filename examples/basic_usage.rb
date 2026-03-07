#!/usr/bin/env ruby
# frozen_string_literal: true

# Usage: ruby examples/basic_usage.rb [amqp_url]
#
# Demonstrates basic MQKV operations against a running AMQP broker.

require "mqkv"

url = ARGV[0] || ENV.fetch("AMQP_URL", "amqp://localhost")
store = MQKV::Store.new(url, prefix: "mqkv-example")

puts "--- SET / GET ---"
store.set("greeting", "hello world")
puts "get(greeting) = #{store.get("greeting").inspect}"

store.set("greeting", "updated!")
puts "get(greeting) = #{store.get("greeting").inspect}"

puts
puts "--- DELETE / EXISTS? ---"
store.set("temp", "will be deleted")
puts "exists?(temp)  = #{store.exists?("temp")}"
store.delete("temp")
puts "after delete:    #{store.exists?("temp")}"
puts "get(temp)      = #{store.get("temp").inspect}"

puts
puts "--- HISTORY ---"
store.set("counter", "1")
store.set("counter", "2")
store.set("counter", "3")
puts "history(counter)            = #{store.history("counter").inspect}"
puts "history(counter, limit: 2)  = #{store.history("counter", limit: 2).inspect}"

store.delete("counter")
store.set("counter", "fresh")
puts "history after delete+set    = #{store.history("counter").inspect}"

puts
puts "--- WATCH ---"
received = []
handle = store.watch("events") { |val| received << val }
sleep 0.1
store.set("events", "event-1")
store.set("events", "event-2")
store.delete("events")
store.set("events", "event-3")
sleep 0.3
store.unwatch(handle)
puts "watched values = #{received.inspect}"

store.purge!
store.close
puts
puts "Done."
