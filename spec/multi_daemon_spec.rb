require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe Monotable::LocalStore do
  include MonotableHelperMethods

  it "should be possible to initialize a new LocalStore in MultiDaemon mode" do
    reset_temp_dir
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    local_store.initialize_new_store
    local_store.chunks.length.should == 5
  end
end
