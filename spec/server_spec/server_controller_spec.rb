require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")

describe Monotable::HttpServer::ServerController do
  include DaemonTestHelper

  def default_options
    {:num_store_paths => 1,:initialize_new_test_store => true}
  end

  def daemon_transaction(options=default_options)
    if options!=default_options
      shutdown_daemon
      start_daemon options
    end
    begin
      yield
    ensure
      shutdown_daemon
    end
  end

  before(:each) do
    start_daemon(:num_store_paths => 1,:initialize_new_test_store => true) unless server_client
  end

  after(:all) do
    shutdown_daemon
  end

  it "GET server/servers should return valid known-servers list" do
    server_client.servers.keys.should == ["127.0.0.1:32100"]
  end

  it "GET server/heartbeat should work" do
    server_client.up?.should == true
  end

  it "GET server/chunks should work" do
    server_client.chunks.should == [""] # a new, one-chunk test-store
  end

  it "GET server/chunk should work" do
    server_client.chunk_status("").should >= {
      range_start:        "",
      record_count:       0,
      accounting_size:    0,
      replication_client: nil,
      replication_source: nil
    } # an empty chunk
  end

  it "server/chunk should work with any key in the chunk" do
    server_client.chunk_status("abc").should >= {"range_start" => "", "record_count" => 0, "accounting_size" => 0} # an empty chunk
  end

  it "GET server/chunk_keys should work" do
    server_client.set "dude", "id" => "123"
    server_client.chunk_keys("").should == ["u/dude"]
    server_client.delete "dude"
  end

  it "GET server/local_store_status should work" do
    status = server_client.local_store_status
    status[:chunk_count].should==1
    status[:record_count].should==0
  end

  it "GET server/chunk should work" do
    # add something to the chunk we are going to up-replicate so we can verify it gets passed through
    test_record = {"field1"=>"value1"}
    server_client.set("foo",test_record)
    server_client["foo"].should==test_record

    # get raw chunk data
    chunk_data = server_client.chunk("")

    # parse and verify returned data
    chunk = Monotable::MemoryChunk.new(:data => chunk_data)
    chunk.range_start.should == ""
    chunk.keys.should == ["u/foo"]
    chunk["u/foo"].should == test_record

    # cleanup
    server_client.chunk_keys("").should == ["u/foo"]
    server_client.delete "foo"
    server_client.chunk_keys("").should == []
  end

  it "POST server/join joining the cluster should add the joining server-name to the known servers list" do
    daemon_transaction do
      server_client.join("frank",["frank"])
      server_client.servers.keys.should == ["127.0.0.1:32100","frank"]
    end
  end

  it "SET server/chunk_replication_client should work" do
    server_client.join("frank",["frank"])

    server_client.chunk_status("").should >= {"replication_client" => nil, "replication_source" => nil}
    server_client.set_chunk_replication("","stella","frank")
    server_client.chunk_status("").should >= {"replication_client" => "frank", "replication_source" => "stella"}
    server_client.set_chunk_replication("","","")
    server_client.chunk_status("").should >= {"replication_client" => nil, "replication_source" => nil}
  end

  it "DELETE server/chunk should work" do
    # add something to the chunk we are going to up-replicate so we can verify it gets passed through
    daemon_transaction do
      server_client.chunks.should == [""]
      server_client.delete_chunk("")
      server_client.chunks.should == []
    end
  end

  it "POST server/split_chunk should work" do
    daemon_transaction do
      server_client.chunks.should == [""]
      server_client.split_chunk("foo")
      server_client.chunks.should == ["","foo"]
    end
  end

  it "POST server/journal_write should work" do
    encoded_journal_entry = Monotable::Journal.encode_journal_entry("set", ["u/my_key", "my_field", "my_value"])
    server_client.journal_write "", encoded_journal_entry
    server_client["my_key"].should=={"my_field"=>"my_value"}
    server_client.delete "my_key"
  end

  it "POST server/clone_chunk should work" do
    daemon_transaction do
      start_daemon(:join=>daemon_address(0)) # start second server

      test_record = {"field1"=>"value1"}
      server_client.set("foo",test_record)

      server_client(0).chunks.should == [""]
      server_client(1).chunks.should == []
      server_client(1).clone_chunk "", daemon_address(0)

      server_client(0).chunks.should == [""]
      server_client(1).chunks.should == [""]

      server_client(0)["foo"].should == test_record
      server_client(1)["foo"].should == test_record
    end
  end

end
