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
    record_value = { 'x' => '1' }
    # First write the record, and make sure it is there
    RestClient.put("#{DAEMON_URI}/records/#{record_key}", record_value.to_json, :content_type => :json, :accept => :json)
    RestClient.get("#{DAEMON_URI}/records/#{record_key}") {|response, request, result|
      response.code.should == 200
    }
    # Then delete the record, and make sure it is not there
    RestClient.delete("#{DAEMON_URI}/records/#{record_key}")
    RestClient.get("#{DAEMON_URI}/records/#{record_key}") {|response, request, result|
      response.code.should == 404
    }
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

  it "should be possible to delete all records in the store" do
    setup_store(2)
    clear_store

    JSON.parse(RestClient.get("#{DAEMON_URI}/first_records/gte"))["records"].length.should == 0
  end

  it "should respond to /first_records/gte" do
    setup_store(9)

    JSON.parse(RestClient.get("#{DAEMON_URI}/first_records/gte", :params => {:limit=>1})).should==
      {"records"=>[["key1", {'field'=>"1"}]], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/first_records/gte", :params => {:limit=>2})).should==
      {"records"=>[["key1", {'field'=>"1"}], ["key2", {'field'=>"2"}]], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/first_records/gte/key2", :params => {:limit=>2})).should==
      {"records"=>[["key2", {'field'=>"2"}],["key3", {'field'=>"3"}]], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/first_records/gte/key1", :params => {:limit=>2})).should==
      {"records"=>[["key1", {'field'=>"1"}], ["key2", {'field'=>"2"}]], "next_options"=>nil, "work_log"=>["processed locally"]}
  end

  it "should respond to /first_records/ge" do
    setup_store(9)

    JSON.parse(RestClient.get("#{DAEMON_URI}/first_records/gt/key1", :params => {:limit=>2})).should==
      {"records"=>[["key2", {'field'=>"2"}],["key3", {'field'=>"3"}]], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/first_records/gt", :params => {:limit=>2})).should==
      {"records"=>[["key1", {'field'=>"1"}], ["key2", {'field'=>"2"}]], "next_options"=>nil, "work_log"=>["processed locally"]}
  end

  it "should respond to /last_records/lte" do
    setup_store(9)

    JSON.parse(RestClient.get("#{DAEMON_URI}/last_records/lte/key0", :params => {:limit=>10})).should==
      {"records"=>[], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/last_records/lte/key1", :params => {:limit=>10})).should==
      {"records"=>[["key1", {'field'=>"1"}]], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/last_records/lte/key2", :params => {:limit=>10})).should==
      {"records"=>[["key1", {'field'=>"1"}],["key2", {'field'=>"2"}]], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/last_records/lte/key3", :params => {:limit=>2})).should==
      {"records"=>[["key2", {'field'=>"2"}],["key3", {'field'=>"3"}]], "next_options"=>nil, "work_log"=>["processed locally"]}
  end

  it "should respond to /last_records/lt" do
    setup_store(9)

    JSON.parse(RestClient.get("#{DAEMON_URI}/last_records/lt/key0", :params => {:limit=>10})).should==
      {"records"=>[], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/last_records/lt/key1", :params => {:limit=>10})).should==
      {"records"=>[], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/last_records/lt/key2", :params => {:limit=>10})).should==
      {"records"=>[["key1", {'field'=>"1"}]], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/last_records/lt/key5", :params => {:limit=>2})).should==
      {"records"=>[["key3", {'field'=>"3"}],["key4", {'field'=>"4"}]], "next_options"=>nil, "work_log"=>["processed locally"]}

    JSON.parse(RestClient.get("#{DAEMON_URI}/last_records/lt/", :params => {:limit=>2})).should==
      {"records"=>[["key8", {'field'=>"8"}],["key9", {'field'=>"9"}]], "next_options"=>nil, "work_log"=>["processed locally"]}
  end

  it "should respond to 406 to invalide requests" do
    expect{RestClient.get("#{DAEMON_URI}/first_records/lte/key1")}.to raise_error(RestClient::NotAcceptable)
  end

  after(:all) do
    # Shut down the daemon
    Process.kill 'HUP', @server_pid

    # Remove the test local store
    FileUtils.rm_rf LOCAL_STORE_PATH
  end
end
