# encoding: BINARY
require File.join(File.dirname(__FILE__),"../lib/monotable/monotable")
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe Monotable::StringBinaryEnumeration do


  it "'hi'.binary_next(10)" do
    "hi".binary_next(10).should=="hi\x00"
  end

  it "''.binary_next(10)" do
    "".binary_next(10).should=="\x00"
  end

  it '"\x00".binary_next(10)' do
    "\x00".binary_next(10).should=="\x00\x00"
  end

  it "binary_prev(10) basic basic" do
    "hi".binary_prev(10).should=="hh\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"
  end

  it "binary_prev(10) basic basic" do
    "hh\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".binary_prev(10).should=="hh\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE"
  end

  it "binary_prev(10) basic base" do
    "\x00".binary_prev(10).should==""
  end

  it "binary_prev(10) basic edge" do
    "hi\x00".binary_prev(10).should=="hi"
  end

  it "should fail to call binary_prev(10) on the empty string. This is the minimum string." do
    lambda { "".binary_prev(10) }.should raise_error(ArgumentError)
  end

  it "should fail to call binary_next(10) on 10 \\0xffs. This is the maximum string." do
    lambda { ("\xff"*10).binary_next(10) }.should raise_error(ArgumentError)
  end

  it "should work to str.binary_next(10).binary_prev(10)" do
    ["","hi","hi\x00","hi\xff","hi"+("\xff"*8),"hi"+("\x00"*8)].each do |a|
      b=a.binary_next(10)
      c=b.binary_prev(10)
      #puts ["nextprev",a,b,c].inspect
      c.should==a
    end
  end

  it "should work to str.binary_prev(10).binary_next(10)" do
    ["hi","hi\x00","hi\xff","\xff"*10,"hi"+("\xff"*8),"hi"+("\x00"*8)].each do |a|
      b=a.binary_prev(10)
      c=b.binary_next(10)
      #puts ["prevnext",a,b,c].inspect
      c.should==a
    end
  end

  it "should test all 0-255 for next, and should always be >" do
  end

  it "should test all prefix+0-255 for prev, and should always be <" do
  end

end
