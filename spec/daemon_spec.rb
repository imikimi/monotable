require File.expand_path(File.join(File.dirname(__FILE__),'..','lib','monotable','monotable'))
require 'rubygems'
require 'rest_client'
require 'tmpdir'
require 'fileutils'
require 'net/http'
require 'json'
require 'uri'

describe Monotable::Daemon do
  PORT = 32100
  HOST = '127.0.0.1'
  DAEMON_URI = "http://#{HOST}:#{PORT}"
  
  before(:all) do
    # Set up the local store
    Monotable::LOCAL_STORE_PATH = Dir.mktmpdir
    Monotable::LOCAL_STORE = Monotable::SoloDaemon.new(
      :store_paths => [Monotable::LOCAL_STORE_PATH],
      :verbose => true
    )
    
    # Start up the daemon
    @server_pid = fork {
      EM.run {
        EM.start_server HOST, PORT,  Monotable::Daemon
      }
    }
    sleep 0.1 # Hack; sleep for a bit while the server starts up
    # @record_resource = RestClient::Resource.new "http://localhost:#{PORT}/records"
  end

  it "should be accessible via HTTP" do
    Net::HTTP.get(HOST,'/',PORT).should_not be_empty
  end
  
  it "returns 404 for a non-existant record" do
    res = Net::HTTP.get_response(HOST,"/records/missing", PORT)
    res.code.should == '404'
  end
  
  it "return the JSON we feed it" do
    record_key = 'apple'
    record_value = { 'x' => '1' }.to_json
    RestClient.put("#{DAEMON_URI}/records/#{record_key}", record_value, :content_type => :json, :accept => :json)
    RestClient.get("#{DAEMON_URI}/records/#{record_key}").should == record_value
    # x = Net::HTTP.put("#{DAEMON_URI}/records/apple", {'abcde' => 'xyzzy'} )
    # puts Net::HTTP.get(HOST,'/records/apple',PORT).inspect
  end

  after(:all) do
    # Shut down the daemon
    Process.kill 'HUP', @server_pid
    
    # Remove the test local store
    FileUtils.rm_rf Monotable::LOCAL_STORE_PATH
  end
end
