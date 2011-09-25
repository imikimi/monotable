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
  LOCAL_STORE_PATH = Dir.mktmpdir

  before(:all) do

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

  it "should be accessible via HTTP" do
    Net::HTTP.get(HOST,'/',PORT).should_not be_empty
  end

  it "returns 404 for a non-existent record" do
    res = Net::HTTP.get_response(HOST,"/records/missing", PORT)
    res.code.should == '404'
  end

  it "return the record we create when using JSON" do
    record_key = 'apple'
    record_value = { 'x' => '1' }
    RestClient.put("#{DAEMON_URI}/records/#{record_key}", record_value.to_json, :content_type => :json, :accept => :json)
    response = JSON.parse(RestClient.get("#{DAEMON_URI}/records/#{record_key}"))
    response["record"].should==record_value
  end

  it "should not return the record after we've deleted it" do
    record_key = 'apple'
    record_value = { 'x' => '1' }.to_json
    # First write the record, and make sure it is there
    RestClient.put("#{DAEMON_URI}/records/#{record_key}", record_value, :content_type => :json, :accept => :json)
    RestClient.get("#{DAEMON_URI}/records/#{record_key}") {|response, request, result|
      response.code.should == 200
    }
    # Then delete the record, and make sure it is not there
    RestClient.delete("#{DAEMON_URI}/records/#{record_key}")
    RestClient.get("#{DAEMON_URI}/records/#{record_key}") {|response, request, result|
      response.code.should == 404
    }
  end

  after(:all) do
    # Shut down the daemon
    Process.kill 'HUP', @server_pid

    # Remove the test local store
    FileUtils.rm_rf LOCAL_STORE_PATH
  end
end
