require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

module Monotable
describe Monotable::Columns do
  include MonotableHelperMethods

  it "should work with one column" do
    cols = Columns.new
    col_info = {"name" => "col_nam", "Content-Type" => "image/jpeg"}
    cols << col_info
    cols.length.should == 1
    cols[0].should == col_info
    cols[col_info].should == 0
  end

  it "should ignore adding the same column twice" do
    cols = Columns.new
    col_info = {"name" => "col_name", "Content-Type" => "image/jpeg"}
    cols << col_info
    cols << col_info
    cols.length.should == 1
  end

  it "should allow two columns with the same name but different properties" do
    cols = Columns.new
    col_info = {"name" => "col_name", "Content-Type" => "image/jpeg"}
    cols << col_info
    col_info = {"name" => "col_name", "Content-Type" => "image/gif"}
    cols << col_info
    cols.length.should == 2
  end

  it "should convert to xbd" do
    cols = Columns.new
    col_info = {"name" => "col_name", "Content-Type" => "image/jpeg"}
    cols << col_info
    xbd_tag = cols.xbd_tag
    bin = xbd_tag.to_binary
    cols2 = Columns.new Xbd.parse(bin)
    cols2.should == cols
  end
end
end
