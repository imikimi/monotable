require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"common_api_tests")

describe Monotable::LocalStore do
  include MonotableHelperMethods

  def blank_store
    reset_temp_dir
    Monotable::LocalStore.new(:store_paths=>[temp_dir],:initialize_new_test_store=>true)
  end

  api_tests
end