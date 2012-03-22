require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.expand_path(File.join(File.dirname(__FILE__),'daemon_test_helper'))

describe Monotable::LocalStore do
  include MonotableHelperMethods
  include DaemonTestHelper

  after(:all) do
    cleanup
  end

  def blank_server
    reset_temp_dir
    Monotable::Server.new(:store_paths=>[local_store_path],:initialize_new_store=>true)
  end

  def blank_server_with_two_path_stores
    Monotable::Server.new(:store_paths=>[local_store_path,local_store_path],:initialize_new_store=>true)
  end

  it "should be possible to read an index record" do
    local_store = blank_server.local_store
    res = local_store.get_last  :limit=>1, :gte=>"", :lte=>"++++0"
    res[:records][0].fields.should=={"servers"=>"localhost:8080"}
  end

  it "should work to have more than one path-store" do
    server = blank_server_with_two_path_stores
    server.local_store.status[:path_stores].length.should == 2
    server.local_store.get_chunk("abc").status.should >= {:range_start => "0", :record_count => 0, :accounting_size => 0} # an empty chunk
  end

=begin
  it "should work to move a chunk to a new path_store" do
    server = blank_server_with_two_path_stores
    local_store = server.local_store

    local_store.status[:path_stores].length.should == 2
    chunk = local_store.get_chunk("abc")

    local_store.status[:path_stores][0][:chunk_count].should == 5
    local_store.status[:path_stores][1][:chunk_count].should == 0

    chunk.move(local_store.path_stores[1])

    local_store.status[:path_stores][0][:chunk_count].should == 5
    local_store.status[:path_stores][1][:chunk_count].should == 0

    local_store.compact

    local_store.status[:path_stores][0][:chunk_count].should == 4
    local_store.status[:path_stores][1][:chunk_count].should == 1
  end
=end
end
