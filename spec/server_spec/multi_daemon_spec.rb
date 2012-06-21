require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")

describe Monotable::LocalStore do
  include MonotableHelperMethods

  it "should be possible to initialize a new LocalStore in MultiDaemon mode" do
    reset_temp_dir
    server = Monotable::Server.new(:store_paths=>[temp_dir], :initialize_new_store => true)
    local_store = server.local_store
    local_store.chunks.length.should == 5
  end
end
