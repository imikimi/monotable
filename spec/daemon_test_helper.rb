module DaemonTestHelper

  def port; 32100; end
  def host; '127.0.0.1'; end
  def daemon_uri(daemon_number=0); "http://#{host}:#{port+daemon_number}"; end
  def server_client(daemon_number=0); server_clients[daemon_number]; end

  def local_store_paths
    @local_store_paths||=[]
  end

  def local_store_path
    ret = Dir.mktmpdir
    local_store_paths << ret
    ret
  end

  def server_pids
    @server_pids||=[]
  end

  def server_clients
    @server_clients||=[]
  end

  def start_daemon(options={:initialize_new_test_store => true})
    # Start up the daemon
    daemon_number = server_pids.length
    server_pids<< fork {
      Monotable::Daemon::Server.start({
        :port=>port + daemon_number,
        :host=>host,
        :store_paths => [local_store_path],
#        :verbose => true,
      }.merge(options)
      )
    }
    server_clients << Monotable::ServerClient.new(daemon_uri(daemon_number))
    sleep 0.1 # Hack; sleep for a bit while the server starts up
  end

  def shutdown_daemon
    server_pids.each do |server_pid|
      Process.kill 'HUP', server_pid
      Process.wait server_pid
    end
    @server_clients = @server_pids=nil
    cleanup
  end

  def cleanup
    local_store_paths.each do |local_store_path|
      FileUtils.rm_rf local_store_path
    end
    @local_store_paths=nil
  end

  def clear_store
    records=JSON.parse(RestClient.get("#{daemon_uri}/first_records/gte",:params => {:limit=>100}))["records"]
    records.each do |k,v|
      RestClient.delete("#{daemon_uri}/records/#{k}")
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
