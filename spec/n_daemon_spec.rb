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

  before(:each) do
    start_daemon(:initialize_new_store=>true,:num_index_levels => 2)
    start_daemon(:join=>daemon_address(0))
    start_daemon(:join=>daemon_address(0))
    start_daemon(:join=>daemon_address(0))
  end

  after(:each) do
    shutdown_daemon
  end

  it "should be possible to start up 2 daemons" do
    server_pids.length.should == 4
    server_client(0).up?.should==true
    server_client(1).up?.should==true
    server_client(2).up?.should==true
    server_client(3).up?.should==true
  end

  it "all servers should know about each other" do
    server_pids.length.should == 4
    servers_servers=server_clients.collect {|a| a.servers}
    servers_servers.each do |servers|
      servers.should == servers_servers[0]
    end
  end

end
