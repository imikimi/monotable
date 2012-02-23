require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"common_api_tests")
require 'rest_client'
require 'tmpdir'
require 'fileutils'
require 'net/http'
require 'json'
require 'uri'
require File.expand_path(File.join(File.dirname(__FILE__),'daemon_test_helper'))

module Monotable
describe ServerClient do
  include DaemonTestHelper
  include MonotableHelperMethods

  before(:all) do
    start_daemon(:initialize_new_store=>true,:num_index_levels => 2)
    start_daemon(:join=>daemon_address(0))
  end

  after(:all) do
    shutdown_daemon
  end

  def client
    server_client(1)
  end

  def blank_store
    clear_store
    server_client(1)
  end

  it "should be accessible via HTTP" do
    server_client(1).set("foo", "fookey" => "fooval")
    server_client(0).chunk_keys("foo").should == ["u/foo"]
    server_client(1).chunk_keys("foo").should == nil
    clear_store
#    server_client(1).delete("foo")
    server_client(0).chunk_keys("foo").should == []
  end

  it_should_behave_like "monotable api",:key_prefix_size => 2

end
end
