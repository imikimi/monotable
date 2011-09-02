require File.expand_path(File.join(File.dirname(__FILE__),'..','lib','monotable','monotable'))
require 'rest_client'

describe Monotable::Daemon do
  PORT = 32100
  HOST = '127.0.0.1'
  
  before(:all) do
    # Start up the daemon
    @server_pid = fork {
      EM.run {
        EM.start_server HOST, PORT,  Monotable::Daemon
      }
    }
    @record_resource = RestClient::Resource.new "http://localhost:#{PORT}/records"
  end

  it "should be accessible via HTTP" do
    Net::HTTP.get(HOST,'/',PORT).should_not be_empty
  end
  
  it "return the JSON we feed it" do
    @record_resource.get
  end

  after(:all) do
    # Shut down the daemon
    Process.kill 'HUP', @server_pid
  end
end
