require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require 'rubygems'
require 'rest_client'
require 'tmpdir'
require 'fileutils'
require 'net/http'
require 'json'
require 'uri'
require File.expand_path(File.join(File.dirname(__FILE__),'daemon_test_helper'))

describe Monotable::EventMachineServer do
  include DaemonTestHelper

  before(:each) do
    start_daemon(:initialize_new_store=>true,:num_index_levels => 2)
    start_daemon(:join=>daemon_address(0))
  end

  after(:each) do
    shutdown_daemon
  end

  it "should be possible to start up 2 daemons" do
    server_pids.length.should == 2
    server_client(0).up?.should==true
    server_client(1).up?.should==true
  end

  it "should be possible replicate" do
    chunk_name = server_client.chunks[-1]
    server_client(0).set_chunk_replication_clients(chunk_name,[daemon_address(1)])
    server_client(0).chunk_info(chunk_name)["replication_clients"].should == [daemon_address(1)]

    server_client(1).chunks.should==[]
    server_client(1).clone_chunk(chunk_name,daemon_address(0))
    server_client(1).chunks.should==[chunk_name]

    server_client(0).chunk_keys(chunk_name).should==[]
    server_client(1).chunk_keys(chunk_name).should==[]

    server_client.set("frank","foo" => "bar")

    server_client(0).chunk_keys(chunk_name).should==["u/frank"]
    server_client(1).chunk_keys(chunk_name).should==["u/frank"]
  end
end
