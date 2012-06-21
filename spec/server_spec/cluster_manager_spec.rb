require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")
require 'rubygems'
require 'rest_client'
require 'tmpdir'
require 'fileutils'
require 'net/http'
require 'json'
require 'uri'

describe Monotable::ClusterManager do
  include DaemonTestHelper

  before(:each) do
    start_daemon(:initialize_new_store=>true,:num_index_levels => 2)
    start_daemon(:join=>daemon_address(0))
  end

  after(:each) do
    shutdown_daemon
  end

  it "cluster_manager should be able to find the first server" do
    server = nil
    Monotable::ServerClient.use_synchrony = false
    cluster_manager = Monotable::ClusterManager.new(server)
    cluster_manager.add daemon_address(1)
    cluster_manager.add daemon_address(0)
    first_server = cluster_manager.locate_first_chunk
    first_server.to_s.should == daemon_address(0)
  end

  it "cluster_manager + global_index should be able to find the first_record" do
    Monotable::ServerClient.use_synchrony = false
    server = Monotable::Server.new
    cluster_manager = server.cluster_manager
    cluster_manager.add daemon_address(1)
    cluster_manager.add daemon_address(0)

    first_record = server.global_index.first_record
    first_record.key.should == "+++0"
    first_record["servers"].should == daemon_address(0)
  end
end
