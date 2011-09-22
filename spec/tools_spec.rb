require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

module Monotable
describe Tools do
  include MonotableHelperMethods

  it "should be possible to convert a directory into a chunk" do
    out_file=chunkify_test_data_directory
    File.exist?(out_file).should == true
  end

  it "should normalize_range_options({})" do
    Tools.normalize_range_options({}).should=={
      :limit=>1,
      :gte=>"",
      :lte=>"\xFF"*DEFAULT_MAX_KEY_LENGTH
    }
  end

  it "should normalize_range_options({:lt, :gt})" do
    Tools.normalize_range_options({:gt=>"a",:lt=>"b"}).should=={
      :limit=>1,
      :gt=>"a",
      :lt=>"b",
      :gte=>"a\x00",
      :lte=>"a"+"\xFF"*(DEFAULT_MAX_KEY_LENGTH-1)
    }
  end
  it "should normalize_range_options({:with_prefix})" do
    Tools.normalize_range_options({:with_prefix=>"foo"}).should=={
      :limit=>1,
      :with_prefix=>"foo",
      :gte=>"foo",
      :lte=>"foo"+"\xFF"*(DEFAULT_MAX_KEY_LENGTH-3)
    }
  end
end
end
