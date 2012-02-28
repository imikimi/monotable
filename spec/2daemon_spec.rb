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

  class ScanningClient
    attr_accessor :clients

    def get_record(key)
      clients.each do |c|
#        puts "#{self.class}.get_record() #{c}.get_record(#{key.inspect})"
        r=c.get_record(key)
        return r if r
      end
      nil
    end
  end

  def validate_index_records_for_chunks_on_server(client,internal_client=nil)
    internal_client||=client.internal
    server_name = client.server.split("http://")[1]
    client.chunks.each do |chunk|
      next if chunk==""
#      puts "validate #{server_name}:#{chunk} index"
      index_record = Monotable::GlobalIndex.index_record(chunk,internal_client)
      index_record.servers.should == [server_name]
    end
  end

  it "a balance request should leave the two servers with near equal chunk counts" do

    server_client(0).chunks.should == ["", "++0", "+0", "0"]
    server_client(1).chunks.should == []

    validate_index_records_for_chunks_on_server server_client(0)

    res = server_client(1).balance
    res[:chunks_moved].length.should == 2

    server_client(0).chunks.should == ["", "++0"]
    server_client(1).chunks.should == ["+0", "0"]
    sc = ScanningClient.new
    sc.clients = [server_client(0).internal,server_client(1).internal]
    validate_index_records_for_chunks_on_server server_client(0),sc
    validate_index_records_for_chunks_on_server server_client(1),sc
  end

  it "forward get requests should work" do
    server_client(0).set "bob", "id" => "123"
    server_client(1).get("bob")[:record].should == {"id" => "123"}
  end

  it "should be able to read records balanced across two daemons" do

    server_client(0).chunks.should == ["", "++0", "+0", "0"]
    server_client(1).chunks.should == []

    records = {
      "amanda"=> {"dog" => "andy"     },
      "bret"=>   {"dog" => "buddy"    },
      "craig"=>  {"dog" => "chuckles" },
      "dan"=>    {"dog" => "dooper"   },
      "evan"=>   {"dog" => "erne"     },
      "frank"=>  {"dog" => "flower"   },
    }

    records.each do |key,fields|
      server_client.set key,fields
    end

    # verify written records before splits
    records.each do |key,fields|
      server_client(0).get(key)[:record].should == fields
    end

    split_keys = records.keys[1..-1].collect {|a|"u/"+a}
    split_keys.each do |key|
      server_client.split_chunk key
    end

    server_client(0).chunks.should == ["", "++0", "+0", "0"]+split_keys

    # verify written records before balance
    records.each do |key,fields|
      server_client(0).get(key)[:record].should == fields
    end

    server_client(0).chunk_keys("u/bret").should == ["u/bret"]
    server_client(1).chunk_keys("u/bret").should == []

    res = server_client(1).balance
    server_client(0).chunks.should == ["", "++0", "+0", "0", "u/bret"]
    server_client(1).chunks.should == ["u/craig", "u/dan", "u/evan", "u/frank"]

    server_client.chunk_keys("u/bret").should == ["u/bret"]
    server_client(1).chunk_keys("u/bret").should == []

    records.each do |key,fields|
      r0 = server_client(0).get(key)
      r1 = server_client(1).get(key)
      r0[:record].should == fields
      r1[:record].should == fields
    end
  end

  it "should be able to write records balanced across two daemons" do

    server_client(0).chunks.should == ["", "++0", "+0", "0"]
    server_client(1).chunks.should == []

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

    # verify chunks after balance
    server_client(0).chunks.should == ["", "++0", "+0", "0", "u/bret"]
    server_client(1).chunks.should == ["u/craig", "u/dan", "u/evan", "u/frank"]

    # set records
    records.each do |key,fields|
      server_client.set key,fields
    end

    # verify one record is on server-0 and another is on server-1
    server_client(0).chunk_keys("u/bret").should == ["u/bret"]
    server_client(1).chunk_keys("u/bret").should == []
    server_client(0).chunk_keys("u/craig").should == []
    server_client(1).chunk_keys("u/craig").should == ["u/craig"]

    # validate we can read we can read each record from either server
    records.each do |key,fields|
      r0 = server_client(0).get(key)
      r1 = server_client(1).get(key)
      r0[:record].should == fields
      r1[:record].should == fields
    end
  end
end
