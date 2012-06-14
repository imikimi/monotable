module DaemonTestHelper

  def port; 32100; end
  def host; '127.0.0.1'; end
  def daemon_address(daemon_number=0); "#{host}:#{port+daemon_number}"; end
  def daemon_uri(daemon_number=0); "http://"+daemon_address(daemon_number); end
  def server_client(daemon_number=0); server_clients[daemon_number]; end


  def local_store_path
    ret = Dir.mktmpdir
    local_store_paths << ret
    ret
  end

  class DaemonTracker
    class << self
      attr_accessor :server_pids, :server_clients, :local_store_paths
    end
  end

  def local_store_paths
    DaemonTracker.local_store_paths||=[]
  end

  def server_pids
    DaemonTracker.server_pids||=[]
  end

  def server_clients
    DaemonTracker.server_clients||=[]
  end

  def wait_for_server_to_start(client,max_wait = 1)
    start_time = Time.now
    while Time.now - start_time < max_wait
      return client if client.up?
      sleep 0.01
    end
    shutdown_daemon
    raise "server failed to start within #{max_wait} second(s)"
  end

  def start_daemon(options={:initialize_new_test_store => true})
    num_store_paths = options[:num_store_paths] || 1
    print "<"
    # Start up the daemon
    daemon_number = server_pids.length
    server_pids << fork {
      Monotable::GoliathServer::HttpServer.start({
#      Monotable::EventMachineServer::HttpServer.start({
        :port=>port + daemon_number,
        :host=>host,
        :store_paths => num_store_paths.times.collect {local_store_path},
#        :verbose => true,
      }.merge(options)
      )
#      SimpleCov.at_exit {SimpleCov.result}
    }
    server_clients << client=Monotable::ServerClient.new(daemon_uri(daemon_number))
    wait_for_server_to_start client
  end

  def shutdown_daemon
    server_pids.each do |server_pid|
      print ">"
      Process.kill 'TERM', server_pid
      Process.wait server_pid
    end
    DaemonTracker.server_clients = DaemonTracker.server_pids = nil
    cleanup
  end

  def cleanup
    local_store_paths.each do |local_store_path|
      FileUtils.rm_rf local_store_path
    end
    DaemonTracker.local_store_paths = nil
  end

  def clear_store
#    puts "clear_store: with_prefix => ''"
    records = server_client.get_first(:with_prefix => '', :limit=>100)[:records]
    records.each do |record|
#      puts "clear_store: #{record.key}"
      server_client.delete(record.key)
    end
  end

  def setup_store(num_keys)
    clear_store

    num_keys.times do |v|
      v=(v+1).to_s
      RestClient.put("#{daemon_uri}/records/key#{v}", {'field' => v}.to_json, :content_type => :json, :accept => :json)
    end
  end
end
