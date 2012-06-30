require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")

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
    server_client(0).chunks.should==["", "++0", "+0", "0"]
    server_client(1).chunks.should==[]
  end

  it "should be possible to up_replicate then down_replicate a chunk" do
    #**************************************
    # up_replicate
    #**************************************
    server_client(0).up_replicate_chunk("0",server_client(1))

    #verify chunk was added and not removed
    server_client(0).chunks.should==["", "++0", "+0", "0"]
    server_client(1).chunks.should==["0"]

    #verify replication is setup correctly
    server_client(0).chunk_status("0").should >= {"replication_client"=>daemon_address(1), "replication_source"=>nil}
    server_client(1).chunk_status("0").should >= {"replication_client"=>nil, "replication_source"=>daemon_address(0)}

    #verify global index record was updated
    server_client.global_index_record("0")[:fields].should >= {"servers"=>"127.0.0.1:32100,127.0.0.1:32101"}

    #**************************************
    # down_replicate
    #**************************************
    server_client(0).down_replicate_chunk("0",server_client(1))

    #verify chunk was removed only from the second server
    server_client(0).chunks.should==["", "++0", "+0", "0"]
    server_client(1).chunks.should==[]

    #verify replication is setup correctly
    server_client(0).chunk_status("0").should >= {"replication_client"=>nil, "replication_source"=>nil}
    server_client(1).chunk_status("0").should == nil

    #verify global index record was updated
    server_client.global_index_record("0")[:fields].should >= {"servers"=>"127.0.0.1:32100"}
  end
end