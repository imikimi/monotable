require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")

describe Monotable::LocalStore do
  include MonotableHelperMethods
  it_should_behave_like "monotable api"

  def blank_store
    reset_temp_dir
    Monotable::LocalStore.new(:store_paths=>[temp_dir],:initialize_new_test_store=>true)
  end
end
