# frozen_string_literal: true

require "securerandom"
require "net/http"
require "json"
require "fileutils"
require "tmpdir"

RSpec.describe "Puma multi-worker integration", :integration do
  before(:all) do
    skip "Set AMQP_URL to run integration tests" unless ENV["AMQP_URL"]
  end

  let(:amqp_url) { ENV.fetch("AMQP_URL") }
  let(:prefix) { "mqkv-puma-#{SecureRandom.hex(4)}" }
  let(:port) { 19_292 }
  let(:workers) { 2 }
  let(:puma_config_path) { File.join(tmpdir, "puma.rb") }
  let(:rack_app_path) { File.join(tmpdir, "config.ru") }
  let(:pidfile) { File.join(tmpdir, "puma.pid") }
  let(:state_path) { File.join(tmpdir, "puma.state") }
  let(:tmpdir) { @tmpdir }

  around do |example|
    @tmpdir = Dir.mktmpdir("mqkv-puma-test-")
    begin
      example.run
    ensure
      stop_puma
      cleanup_queues
      FileUtils.rm_rf(@tmpdir)
    end
  end

  def write_rack_app
    File.write(rack_app_path, <<~RUBY)
      require "bundler/setup"
      require "mqkv"
      require "json"

      AMQP_URL = ENV.fetch("AMQP_URL")
      PREFIX   = ENV.fetch("MQKV_PREFIX")
      STORE    = MQKV::Store.new(AMQP_URL, prefix: PREFIX, read_timeout: 0.3)

      app = proc do |env|
        req_path = env["PATH_INFO"]
        params   = Rack::Utils.parse_query(env["QUERY_STRING"])

        case [env["REQUEST_METHOD"], req_path]
        when ["POST", "/set"]
          STORE.set(params["key"], params["value"])
          [200, { "content-type" => "application/json" }, [JSON.generate(status: "ok")]]
        when ["GET", "/get"]
          val = STORE.get(params["key"])
          [200, { "content-type" => "application/json" }, [JSON.generate(value: val, pid: Process.pid)]]
        when ["POST", "/preload"]
          keys = params["keys"].split(",")
          STORE.preload(*keys)
          [200, { "content-type" => "application/json" }, [JSON.generate(status: "preloaded", pid: Process.pid)]]
        when ["GET", "/pid"]
          [200, { "content-type" => "application/json" }, [JSON.generate(pid: Process.pid)]]
        else
          [404, { "content-type" => "text/plain" }, ["not found"]]
        end
      end

      run app
    RUBY
  end

  def write_puma_config
    File.write(puma_config_path, <<~RUBY)
      workers #{workers}
      port #{port}
      pidfile "#{pidfile}"
      state_path "#{state_path}"
      environment "production"
    RUBY
  end

  def start_puma
    write_rack_app
    write_puma_config

    @puma_pid = spawn(
      { "AMQP_URL" => amqp_url, "MQKV_PREFIX" => prefix },
      "bundle", "exec", "puma", "-C", puma_config_path, rack_app_path,
      chdir: "/app",
      out: File.join(tmpdir, "puma.stdout.log"),
      err: File.join(tmpdir, "puma.stderr.log")
    )
    Process.detach(@puma_pid)
    wait_for_server
  end

  def stop_puma
    return unless @puma_pid

    Process.kill("TERM", @puma_pid)
    Process.wait(@puma_pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end

  def cleanup_queues
    store = MQKV::Store.new(amqp_url, prefix: prefix, read_timeout: 0.1)
    store.get("shared-key") # ensure stream is declared so purge can find it
    store.purge!
    store.close
  rescue StandardError
    nil
  end

  def wait_for_server(timeout: 15)
    deadline = Time.now + timeout
    loop do
      Net::HTTP.get(URI("http://127.0.0.1:#{port}/pid"))
      return
    rescue Errno::ECONNREFUSED, Errno::ECONNRESET, Net::ReadTimeout
      raise "Puma did not start within #{timeout}s" if Time.now > deadline
      sleep 0.2
    end
  end

  def http_get(path)
    uri = URI("http://127.0.0.1:#{port}#{path}")
    response = Net::HTTP.get_response(uri)
    JSON.parse(response.body)
  end

  def http_post(path)
    uri = URI("http://127.0.0.1:#{port}#{path}")
    response = Net::HTTP.post(uri, "")
    JSON.parse(response.body)
  end

  def collect_worker_pids(samples: 20)
    pids = Set.new
    samples.times do
      result = http_get("/pid")
      pids << result["pid"]
    end
    pids
  end

  it "set in one worker is visible from another worker via the shared broker" do
    start_puma

    # Verify we have multiple worker processes
    pids = collect_worker_pids(samples: 30)
    expect(pids.size).to be >= 2, "Expected requests served by at least 2 workers, got pids: #{pids.to_a}"

    # Set a value (will hit one worker)
    http_post("/set?key=shared-key&value=hello-from-puma")

    # Read it back enough times to hit a different worker
    values = Set.new
    30.times do
      sleep 0.05
      result = http_get("/get?key=shared-key")
      values << result["value"] if result["value"]
    end

    expect(values).to include("hello-from-puma")
  end
end
