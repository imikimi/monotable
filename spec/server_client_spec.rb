require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"common_api_tests")
require 'rest_client'
require 'tmpdir'
require 'fileutils'
require 'net/http'
require 'json'
require 'uri'
require File.expand_path(File.join(File.dirname(__FILE__),'daemon_test_helper'))

module Monotable
describe ServerClient do
  include DaemonTestHelper
  include MonotableHelperMethods

  before(:all) do
    start_daemon
  end

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

  it "should be accessible via HTTP" do
    Net::HTTP.get(host,'/',port).should_not be_empty
  end

  api_tests(:key_prefix_size => 2)

  it "should be able to create a ServerClient" do
    ServerClient.new(daemon_uri)
  end

  it "should be able to get a record" do
    setup_store(3)
    client.get("key1").should=={:record=>{"field"=>"1"}, :size=>12, :num_fields=>1, :work_log=>["processed locally"]}
    client["key1"].should=={"field"=>"1"}
  end

  it "should be able to set a record with set" do
    clear_store
    client.set("foo",{"bar"=>"monkey"}).should=={:result=>:created, :size_delta=>14, :size=>14, :work_log=>["processed locally"]}
    client["foo"].should=={"bar" => "monkey"}
  end

  it "should be able to set a record with []" do
    clear_store
    client["foo"]={"bar"=>"monkey"}
    client["foo"].should=={"bar" => "monkey"}
  end
end
end
