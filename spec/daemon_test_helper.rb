module DaemonTestHelper
  PORT = 32100
  HOST = '127.0.0.1'
  DAEMON_URI = "http://#{HOST}:#{PORT}"
  LOCAL_STORE_PATH = Dir.mktmpdir

  def port; PORT; end
  def host; HOST; end
  def daemon_uri; DAEMON_URI; end

  def start_daemon
    # Start up the daemon
    @server_pid = fork {
      Monotable::Daemon::Server.start(
        :port=>PORT,
        :host=>HOST,
        :store_paths => [LOCAL_STORE_PATH],
#        :verbose => true,
        :initialize_new_store => true
      )
    }
    sleep 0.1 # Hack; sleep for a bit while the server starts up
  end

  def shutdown_daemon
    Process.kill 'HUP', @server_pid
  end

  def cleanup
    FileUtils.rm_rf LOCAL_STORE_PATH
  end

  def clear_store
    records=JSON.parse(RestClient.get("#{DAEMON_URI}/first_records/gte",:params => {:limit=>100}))["records"]
    records.each do |k,v|
      RestClient.delete("#{DAEMON_URI}/records/#{k}")
    end
  end

  def setup_store(num_keys)
    clear_store

    num_keys.times do |v|
      v=(v+1).to_s
      RestClient.put("#{DAEMON_URI}/records/key#{v}", {'field' => v}.to_json, :content_type => :json, :accept => :json)
    end
  end
end
