require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")
require 'rest_client'
require 'tmpdir'
require 'fileutils'
require 'net/http'
require 'json'
require 'uri'

module Monotable
describe ServerClient do
  include DaemonTestHelper
  include MonotableHelperMethods
  it_should_behave_like "monotable api", :key_prefix_size => 2

  before(:all) do
    start_daemon
  end

  before(:each) do
    @client=ServerClient.new(daemon_uri)
  end
  attr_accessor :client

  after(:all) do
    shutdown_daemon
  end

  def client
    ServerClient.new(daemon_uri)
  end

  def blank_store
    clear_store
    ServerClient.new(daemon_uri)
  end

  it "should fail locally to set with invalid fields" do
    lambda{client.set "my_key", nil}.should raise_error(Monotable::ArgumentError)
    lambda{client.set "my_key", []}.should raise_error(Monotable::ArgumentError)
  end

  it "should be comparable" do
    client.should == client
    client.should_not == ServerClient.new(daemon_uri(1))
  end

  it "should be accessible via HTTP" do
    Net::HTTP.get(host,'/',port).should_not be_empty
  end

  it "Invalid HTTP posts should return invalid argument errors" do
    RestClient::Request.execute(
      :method => :post,
      :url => "http://#{host}:#{port}/records/my_key",
      :payload => nil.to_json,
      :headers => {:accept => :json, :content_type => :json}
    ) do |response, request, result|
      result.code.should=="400"
    end
  end


  it "should be able to create a ServerClient" do
    client
  end

  it "should be able to get a record" do
    setup_store(3)
    client.get("key1").should>={:record=>{"field"=>"1"}, :size=>12, :num_fields=>1}
    client["key1"].should=={"field"=>"1"}
  end

  it "should be able to set a record with set" do
    clear_store
    client.set("foo",{"bar"=>"monkey"}).should>={:result=>"created", :size_delta=>14, :size=>14}
    client["foo"].should=={"bar" => "monkey"}
  end

  it "should be able to set a record with []" do
    clear_store
    client["foo"]={"bar"=>"monkey"}
    client["foo"].should=={"bar" => "monkey"}
  end

  it "should be able to set/get a single large binary field" do
    clear_store
    data = load_test_data "0-65535.words.binary"
    client.update_field("foo","file",data)
    f=client.get_field("foo","file")
    f.should == data
  end
end
end
