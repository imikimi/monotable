require File.expand_path(File.join(File.dirname(__FILE__),'..','lib','monotable','monotable'))
require 'rubygems'
require 'rest_client'
require 'tmpdir'
require 'fileutils'
require 'net/http'
require 'json'
require 'uri'
require File.expand_path(File.join(File.dirname(__FILE__),'daemon_test_helper'))
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"common_api_tests")

describe Monotable::RequestRouter do
  include MonotableHelperMethods
  include DaemonTestHelper

  before(:all) do
    start_daemon(:initialize_new_store=>true)
  end

  after(:all) do
    shutdown_daemon
  end

  def blank_store

    reset_temp_dir
    server = Monotable::Server.new(:host => "testhost", :port=>"12345")
    server.cluster_manager.join daemon_address(0)
    server.cluster_manager.servers.keys.should == ["testhost:12345", "127.0.0.1:32100"]
    Monotable::RequestRouter.new(server.router, :user_keys=>true, :forward => true)
  end

  it "basic test that the router can connect to the remote server" do
    blank_store
  end

  #it_should_behave_like "monotable api"
end
