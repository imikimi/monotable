require File.join(File.dirname(__FILE__),"xbd_test_helper")
require "fileutils"

module Xbd
describe Tag do
  it "test the various creation methods" do
    Tag.new("my_tag").attrs.should=={}
    Tag.new("my_tag",{:foo=>:bar}).attrs.should=={"foo"=>"bar"}
    Tag.new("my_tag",{:foo=>:bar},[Tag.new("my_other_tag")]).attrs.should=={"foo"=>"bar"}
    Tag.new("my_tag") do |tag|
      tag[:foo]=:bar
    end.attrs.should=={"foo"=>"bar"}
  end

  it "test attrs" do
    tag=Tag.new("my_tag")
    tag.attrs.should=={}
    tag[:foo]=:bar
    tag["foo"].should=="bar"
    tag.attrs.should=={"foo"=>"bar"}
    tag[:foo]=:bar2
    tag["foo"].should=="bar2"
    tag.attrs.should=={"foo"=>"bar2"}
    tag[:foo2]=:bar3
    tag["foo2"].should=="bar3"
    tag.attrs.should=={"foo"=>"bar2","foo2"=>"bar3"}
  end

  it "test tags" do
    tag=full_test_tag
    tag.tagnames.should==["sub1","sub2"]
    tag.tag("sub1")["sub1k"].should==nil
    tag.tag("sub2")["sub2k"].should=="sub2v"
    tag.each_tag("sub1") do |t|
      t.name.should=="sub1"
    end
  end

  it "should convert to XML" do
    tag=full_test_tag
    tag.tag("sub1").to_s.strip.should=='<sub1/>'
    tag.tag("sub2").to_s.strip.should=='<sub2 sub2k="sub2v"/>'
    tag.to_s.gsub(/\s+/," ").strip.should=='<my_tag a1="v1"> <sub1/> <sub2 sub2k="sub2v"/> </my_tag>'
    tag.inspect
  end

  it "should convert to Ruby Hashes" do
    tag=full_test_tag

    tag.to_ruby.should=={
      :name=>"my_tag",
      :attrs=>{"a1"=>"v1"},
      :tags=>[
        {:name=>"sub1", :attrs=>{}, :tags=>[]},
        {:name=>"sub2", :attrs=>{"sub2k"=>"sub2v"}, :tags=>[]}
      ]
    }
  end

  it "should work to convert to xbd" do
    tag=full_test_tag
    xbd=tag.to_binary
    tag2=Xbd.parse(xbd)
    tag.to_s.should==tag2.to_s
  end

  it "should work to store binaries" do
    tag1=full_test_tag
    tag1["nested_xbd"]=full_test_tag.to_binary
    tag2=Xbd.parse(tag1.to_binary)
    tag2["nested_xbd"].should==tag1["nested_xbd"]
    Xbd.parse(tag2["nested_xbd"]).should==full_test_tag
  end

  it "should work to == and !=" do
    tag1=full_test_tag
    tag2=full_test_tag
    (tag1!=tag2).should==false
    (tag1==tag2).should==true

    # test diff tags
    tag2.tags.reverse!
    (tag1!=tag2).should==true
    (tag1==tag2).should==false

    # test diff names
    tag2=full_test_tag
    tag2.name="nameeroo"
    (tag1!=tag2).should==true
    (tag1==tag2).should==false

    # test diff attrs
    tag2=full_test_tag
    tag2["foomagoo"]="goofoo"
    (tag1!=tag2).should==true
    (tag1==tag2).should==false
  end

  it "should work to load from file" do
    begin
      filename=File.join(File.dirname(__FILE__),"xbd_test_file.xbd")
      tag=full_test_tag
      File.open(filename,"wb") {|file| file.write(tag.to_binary)}
      File.exists?(filename).should==true
      tag2=Xbd.load_from_file(filename)
      tag2.should==tag
    ensure
      FileUtils.rm filename
    end
  end

  it "should work to convert to/from binary" do
    tagsd=Dictionary.new
    attrsd=Dictionary.new
    valuesd=Dictionary.new
    tag=full_test_tag
    tag.populate_dictionaries(tagsd,attrsd,valuesd)
    bin=tag.to_binary_partial(tagsd,attrsd,valuesd)
    tag2,index=Tag.parse(bin,0,tagsd,attrsd,valuesd)
    index.should==bin.length
    tag.to_s.should==tag2.to_s
  end

  def full_test_tag
    Tag.new("my_tag") do |tag|
      tag["a1"]="v1"
      tag<<Tag.new("sub1")
      tag<<Tag.new("sub2",{:sub2k=>:sub2v})
    end
  end
end
end
