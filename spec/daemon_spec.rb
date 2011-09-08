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
  end

  it "should be accessible via HTTP" do
    Net::HTTP.get(HOST,'/',PORT).should_not be_empty
  end
  
  it "returns 404 for a non-existent record" do
    res = Net::HTTP.get_response(HOST,"/records/missing", PORT)
    res.code.should == '404'
  end
  
  it "return the record we create when using JSON" do
    record_key = 'apple'
    record_value = { 'x' => '1' }.to_json
    RestClient.put("#{DAEMON_URI}/records/#{record_key}", record_value, :content_type => :json, :accept => :json)
    RestClient.get("#{DAEMON_URI}/records/#{record_key}").should == record_value
  end
  
  it "should not return the record after we've deleted it" do
    record_key = 'apple'
    record_value = { 'x' => '1' }.to_json
    RestClient.put("#{DAEMON_URI}/records/#{record_key}", record_value, :content_type => :json, :accept => :json)
    RestClient.delete("#{DAEMON_URI}/records/#{record_key}")
    RestClient.get("#{DAEMON_URI}/records/#{record_key}") {|response, request, result|
      response.code.should == 200
    }
    Net::HTTP.get_response(HOST,"/records/missing", PORT)    
  end

  after(:all) do
    # Shut down the daemon
    Process.kill 'HUP', @server_pid
    
    # Remove the test local store
    FileUtils.rm_rf Monotable::LOCAL_STORE_PATH
  end
end
