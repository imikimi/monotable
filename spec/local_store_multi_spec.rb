require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe Monotable::LocalStore do
  include MonotableHelperMethods

  def blank_server
    reset_temp_dir
    Monotable::Server.new(:store_paths=>[temp_dir],:initialize_new_store=>true)
  end

  it "should be possible to read an index record" do
    local_store = blank_server.local_store
    res = local_store.get_last  :limit=>1, :gte=>"", :lte=>"++++0"
    res[:records][0].fields.should=={"servers"=>"localhost:8080"}
  end
end
