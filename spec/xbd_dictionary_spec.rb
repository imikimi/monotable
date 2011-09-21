require File.join(File.dirname(__FILE__),"xbd_test_helper")

module Xbd
describe Dictionary do
  it "should be possible to create a dictionary" do
    Dictionary.new.should_not == nil
  end

  it "should return nils for non-existant entries" do
    dict=Dictionary.new
    dict["not_there"].should==nil
  end

  it "should be enumerate words added to it" do
    dict=Dictionary.new
    dict<<"word"
    dict<<"with"
    dict<<"you"

    dict["word"].should==0
    dict["with"].should==1
    dict["you"].should==2

    dict[0].should=="word"
    dict[1].should=="with"
    dict[2].should=="you"
  end

  it "should accept non-string values and convert them to strings" do
    dict=Dictionary.new
    dict<<:word
    dict["word"].should==0
  end

  it "should work to_bin and parse" do
    dict=Dictionary.new
    dict<<"foo"
    dict<<"bar"
    dict["foo"].should==0
    dict["bar"].should==1
    dict[0].should=="foo"
    dict[1].should=="bar"

    bin=dict.to_binary
    dict2,next_index=Dictionary.parse(bin)
    next_index.should==bin.length
    dict2["foo"].should==0
    dict2["bar"].should==1
  end
end
end
