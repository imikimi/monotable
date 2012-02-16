require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"common_api_tests")

describe Monotable::RequestRouter do
  include MonotableHelperMethods

  def blank_store
    reset_temp_dir
    server = Monotable::Server.new(:store_paths=>[temp_dir],:initialize_new_test_store=>true)
    Monotable::RequestRouter.new(server.router, :user_keys=>true, :forward => true)
  end

  api_tests(:dont_test_get_record=>true,:key_prefix_size=>2)

end
