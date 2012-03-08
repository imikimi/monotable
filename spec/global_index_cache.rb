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
  end

  after(:each) do
    shutdown_daemon
  end

  it "should be possible to start up 2 daemons" do
    server_pids.length.should == 2
    server_client(0).up?.should==true
    server_client(1).up?.should==true
  end

  it "should work to cache things" do
    records = {
      "amanda"=> {"dog" => "andy"     },
      "bret"=>   {"dog" => "buddy"    },
      "craig"=>  {"dog" => "chuckles" },
      "dan"=>    {"dog" => "dooper"   },
      "evan"=>   {"dog" => "erne"     },
      "frank"=>  {"dog" => "flower"   },
    }

    # split chunks
    split_keys = records.keys[1..-1]
    split_keys.each do |key|
      ikey = "u/#{key}"
      server_client.set ikey,records[key]
      server_client.split_chunk ikey
    end

    first = server_client(1).get("amanda")
    second = server_client(1).get("amanda")
    puts first.inspect
    puts second.inspect
    first.should == []
  end
end
