require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"common_api_tests")

describe Monotable::ExternalRequestRouter do
  include MonotableHelperMethods

  def blank_store
    reset_temp_dir
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir],:initialize_new_test_store=>true)
    router=Monotable::Router.new(:local_store=>local_store)
    Monotable::InternalRequestRouter.new(router)
  end

  api_tests(:dont_test_get_record=>true)

end
