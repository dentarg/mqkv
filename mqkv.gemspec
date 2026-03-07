# frozen_string_literal: true

require_relative "lib/mqkv/version"

Gem::Specification.new do |spec|
  spec.name = "mqkv"
  spec.version = MQKV::VERSION
  spec.authors = ["mqkv contributors"]
  spec.summary = "Key-value store backed by AMQP stream queues"
  spec.description = "Uses AMQP stream queues (RabbitMQ 3.9+ / LavinMQ) as a key-value store. " \
                     "Each key maps to a dedicated stream queue; the latest message is the current value."
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.3"
  spec.files = Dir["lib/**/*.rb"]
  spec.require_paths = ["lib"]
  spec.add_dependency "amqp-client", "~> 2.0"
end
