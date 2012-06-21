require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")

describe Monotable::RequestRouter do
  include MonotableHelperMethods

  def blank_store
    reset_temp_dir
    server = Monotable::Server.new(:store_paths=>[temp_dir],:initialize_new_test_store=>true)
    Monotable::RequestRouter.new(server.router)
  end

  it_should_behave_like "monotable api"

end
