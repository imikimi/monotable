require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"common_api_tests")

describe Monotable::LocalStore do
  include MonotableHelperMethods

  def blank_store
    reset_temp_dir
    server = Monotable::Server.new(:store_paths=>[temp_dir],:initialize_new_store=>true)
    server.local_store
  end

  it "should be possible to read an index record" do
    local_store = blank_store
    res = local_store.get_last  :limit=>1, :gte=>"", :lte=>"++++0"
    res[:records][0].fields.should=={"servers"=>"localhost:8080"}
  end

  it_should_behave_like "monotable api"
end
