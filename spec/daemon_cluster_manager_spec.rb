require File.expand_path(File.join(File.dirname(__FILE__),'..','lib','monotable','monotable'))
require 'rubygems'
require 'rest_client'
require 'tmpdir'
require 'fileutils'
require 'net/http'
require 'json'
require 'uri'
require File.expand_path(File.join(File.dirname(__FILE__),'daemon_test_helper'))

describe Monotable::Daemon do
  include DaemonTestHelper

  before(:all) do
    start_daemon
  end

  after(:all) do
    shutdown_daemon
  end

  it "should return valid known-servers list" do
    server_client.servers.keys.should == ["127.0.0.1:32100"]
  end

  it "joining the cluster should add the joining server-name to the known servers list" do
    server_client.join("frank")
    server_client.servers.keys.should == ["127.0.0.1:32100","frank"]
  end

end
