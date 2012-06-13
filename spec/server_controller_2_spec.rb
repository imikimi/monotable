require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require 'rubygems'
require 'rest_client'
require 'tmpdir'
require 'fileutils'
require 'net/http'
require 'json'
require 'uri'
require File.expand_path(File.join(File.dirname(__FILE__),'daemon_test_helper'))
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe Monotable::HttpServer::ServerController do
  include DaemonTestHelper

  before(:all) do
    start_daemon(:initialize_new_store=>true)
  end

  after(:all) do
    shutdown_daemon
  end

  it "server/down_replicate_chunk should work" do

    server_client.chunks.should == ["", "+++0", "++0", "+0", "0"]
    server_client.chunk("0").should >= {"range_start"=>"0", "range_end"=>Monotable::LAST_POSSIBLE_KEY}

    server_client.split_chunk("foo")

    server_client.chunks.should == ["", "+++0", "++0", "+0", "0", "foo"]
    server_client.chunk("0").should >= {"range_start"=>"0", "range_end"=>"foo"}
    server_client.chunk("foo").should >= {"range_start"=>"foo", "range_end"=>Monotable::LAST_POSSIBLE_KEY}

    server_client.down_replicate_chunk("foo")

    server_client.chunks.should == ["", "+++0", "++0", "+0", "0"]
    server_client.chunk("0").should >= {"range_start"=>"0", "range_end"=>"foo"}
    server_client.chunk("foo").should == nil

  end

end
