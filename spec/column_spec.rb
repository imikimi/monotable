require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

module Monotable
describe Monotable::Column do
  include MonotableHelperMethods

  def test_name; "username"; end
  it "should accept hash and reply with correct name" do
    col = Column.new "name" => test_name
    col.name.should == test_name
  end

  it "should respond to to_s" do
    col = Column.new test_name
    col.to_s.should == test_name
  end

  it "should be equal if as long as name is squal" do
    col1 = Column.new "name" => test_name, "Content-Type" => "image/jpeg"
    col2 = Column.new "name" => test_name, "Content-Type" => "image/jpeg"
    col1.should==col2

    col1 = Column.new "name" => test_name+"A", "Content-Type" => "image/jpeg"
    col2 = Column.new "name" => test_name+"B", "Content-Type" => "image/jpeg"
    col1.should_not==col2
  end

end
end
