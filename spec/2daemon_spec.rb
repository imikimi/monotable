require File.expand_path(File.join(File.dirname(__FILE__),'..','lib','monotable','monotable'))
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

  before(:all) do
    start_daemon(:initialize_new_store=>true)
    start_daemon(:join=>daemon_address(0))
  end

  after(:all) do
    shutdown_daemon
  end

  it "should be possible to start up 2 daemons" do
    server_pids.length.should == 2
    server_client(0).up?.should==true
    server_client(1).up?.should==true
  end

  it "only the first daemon should start with chunks" do
    local_store1_stats=server_client(0).local_store_status
    local_store2_stats=server_client(1).local_store_status

    local_store1_stats[:chunk_count].should>0
    local_store2_stats[:chunk_count].should==0
  end

  it "both daemons should have different local stores" do
    local_store1_stats=server_client(0).local_store_status
    local_store2_stats=server_client(1).local_store_status
    local_store1_stats[:store_paths].should_not==local_store2_stats[:store_paths]
  end

  it "a balance request should leave the two servers with near equal chunk counts" do
    server_client(0).chunks.length.should>1
    server_client(1).chunks.length.should==0
    (server_client(0).chunks.length - server_client(1).chunks.length).abs.should > 1

    res = server_client(1).balance
    res[:chunks_moved].length.should > 0

    (server_client(0).chunks.length - server_client(1).chunks.length).abs.should <= 1
  end
end
