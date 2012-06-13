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

    server_pids.length.should == 4
    servers_servers=server_clients.collect {|a| a.servers}
    servers_servers.each do |servers|
      servers.should == servers_servers[0]
    end
  end

  it "shoudl work to balance 4 servers" do
    records = {
      "amanda"=> {"dog" => "andy"     },
      "bret"=>   {"dog" => "buddy"    },
      "craig"=>  {"dog" => "chuckles" },
      "dan"=>    {"dog" => "dooper"   },
      "evan"=>   {"dog" => "erne"     },
      "frank"=>  {"dog" => "flower"   },
    }

    # split chunks
    split_keys = records.keys[1..-1].collect {|a|"u/"+a}
    split_keys.each do |key|
      server_client.split_chunk key
    end

    # verify chunks before balance
    server_client(0).chunks.should == ["", "++0", "+0", "0"]+split_keys

    #balance
    res = server_client(1).balance
    res = server_client(2).balance
    res = server_client(3).balance

    # verify chunks after balance
    server_clients.collect{|c|c.chunks}.should == [
      ["", "++0", "+0"],
      ["u/craig", "u/dan"],
      ["0", "u/bret"],
      ["u/evan", "u/frank"]
    ]

    # set records
    records.each do |key,fields|
      server_client.set key,fields
    end

    # verify records can be read from the correct servers
    server_clients.collect{|c|c.chunk_keys("u/bret")}.should == [[],[],["u/bret"],[]]
    server_clients.collect{|c|c.chunk_keys("u/dan")}.should == [[],["u/dan"],[],[]]
    server_clients.collect{|c|c.chunk_keys("u/frank")}.should == [[],[],[],["u/frank"]]
  end
end
