require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.expand_path(File.join(File.dirname(__FILE__),'daemon_test_helper'))
require 'find'

describe Monotable::PathStoreBalancer do
  include MonotableHelperMethods
  include DaemonTestHelper

  after(:all) do
    cleanup
  end

  def blank_server_with_two_path_stores(options={})
    Monotable::Server.new options.merge(
      :store_paths => [local_store_path,local_store_path],
      :initialize_new_store => true
      )
  end


  it "balancer should detect we are unbalanced" do
    server = blank_server_with_two_path_stores(
      :file_system => VirtualSizeFileSystemMock.new(10*1024**2) # 10 megs
    )
    server.local_store.status[:path_stores].length.should == 2
    balancer = Monotable::PathStoreBalancer.new server.local_store
    balancer.max_free_space_delta.should > 0
    balancer.max_balanced_path_store_free_space_delta = 100
    balancer.unbalanced?.should == true
  end

  it "balancer should move chunks to resolve the imbalance" do
    server = blank_server_with_two_path_stores(
      :file_system => VirtualSizeFileSystemMock.new(10*1024) # 10k
      )
    local_store = server.local_store

    local_store.status[:path_stores].length.should == 2
    balancer = Monotable::PathStoreBalancer.new local_store
    balancer.max_balanced_path_store_free_space_delta = 200

    local_store.path_stores[0].chunks.length.should == 3
    local_store.path_stores[1].chunks.length.should == 2

    local_store.path_stores[1].chunks.each {|key,chunk|chunk.move local_store.path_stores[0]}
    local_store.compact

    local_store.path_stores[0].chunks.length.should == 5
    local_store.path_stores[1].chunks.length.should == 0

    balancer.balance
    local_store.compact
    local_store.path_stores[0].chunks.length.should == 3
    local_store.path_stores[1].chunks.length.should == 2
  end

end
