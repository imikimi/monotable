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

  #*******************************************************
  # test get
  #*******************************************************
  it "should get existing records" do
    EventMachine.run do
      store=setup_store_with_test_keys
      store.get("key1") do |response|
        key_prefix_size = 2
        response.should>={:record=>{"data"=>"key1"}, :size=>12+key_prefix_size, :num_fields=>1}
        EventMachine.stop
      end
    end
  end

  it "should get missing records" do
    EventMachine.run do
      store=setup_store_with_test_keys
      store.get("missing") do |response|
        response.should>={:record=>nil}
        EventMachine.stop
      end
    end
  end
end
end
