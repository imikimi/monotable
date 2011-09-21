require File.join(File.dirname(__FILE__),"../lib/monotable/monotable")

describe Monotable::Tools do
  include MonotableHelperMethods

  it "should be possible to convert a directory into a chunk" do
    out_file=chunkify_test_data_directory
    File.exist?(out_file).should == true
  end

end
