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

  def validate_index_records_for_chunks_on_server(client)
    server_name = client.server.split("http://")[1]
    client.chunks.each do |chunk|
      next if chunk==""
      puts "validate #{server_name}:#{chunk} index"
      index_record = Monotable::GlobalIndex.index_record(chunk,client.internal)
      index_record.servers.index(server_name).should >= 0
    end
  end

  it "a balance request should leave the two servers with near equal chunk counts" do

    server_client(0).chunks.should == ["", "+++0", "++0", "+0", "0"]
    server_client(1).chunks.should == []

    validate_index_records_for_chunks_on_server server_client(0)

    res = server_client(1).balance
    res[:chunks_moved].length.should == 3

    server_client(0).chunks.should == ["", "+++0"]
    server_client(1).chunks.should == ["++0", "+0", "0"]
    validate_index_records_for_chunks_on_server server_client(0)
    validate_index_records_for_chunks_on_server server_client(1)
  end
end
