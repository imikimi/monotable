module DaemonTestHelper

  def port; 32100; end
  def host; '127.0.0.1'; end
  def daemon_uri; "http://#{host}:#{port}"; end

  def local_store_path
    @local_store_path ||= Dir.mktmpdir
  end

  def start_daemon
    # Start up the daemon
    @server_pid = fork {
      Monotable::Daemon::Server.start(
        :port=>port,
        :host=>host,
        :store_paths => [local_store_path],
#        :verbose => true,
        :initialize_new_store => true
      )
    }
    sleep 0.1 # Hack; sleep for a bit while the server starts up
  end

  def shutdown_daemon
    Process.kill 'HUP', @server_pid
    Process.wait @server_pid
  end

  def cleanup
    FileUtils.rm_rf local_store_path
    @local_store_path=nil
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
