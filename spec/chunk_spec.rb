require File.join(File.dirname(__FILE__),"../lib/monotable/monotable")
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe Monotable::MemoryChunk do
  include MonotableHelperMethods

  it "should be possible to create an in-memory chunk from scratch" do
    chunk=Monotable::MemoryChunk.new
    chunk.should_not == nil
  end

  it "should be possible to serialize and deserialize a chunk with one record, one column" do
    chunk=Monotable::MemoryChunk.new
    chunk["test_key"]= {"test_column"=>"test_value"}
    chunk_str=chunk.to_binary

    chunk2=Monotable::MemoryChunk.new(:data=>chunk_str)
    chunk2.length.should == 1
    chunk2["test_key"].should == {"test_column"=>"test_value"}
  end

  it "should return nil if key is not in chunk" do
    chunk=Monotable::MemoryChunk.new
    chunk["test_key"]= {"test_column"=>"test_value"}
    chunk_str=chunk.to_binary

    chunk2=Monotable::MemoryChunk.new(:data=>chunk_str)
    chunk2["test_key2"].should == nil
  end

  it "should be possible to de/serialize a chunk with multiple records" do
    chunk=Monotable::MemoryChunk.new
    chunk["key1"]={"col1"=>"val1","col2"=>"val2"}
    chunk["key2"]={"col2"=>"val3","col3"=>"val4"}
    chunk_str=chunk.to_binary

    chunk2=Monotable::MemoryChunk.new(:data=>chunk_str)
    chunk2.length.should == 2
    chunk2["key1"].should == {"col1"=>"val1","col2"=>"val2"}
    chunk2["key2"].should == {"col2"=>"val3","col3"=>"val4"}
  end

  it "should be possible to store binary data in a chunk column" do
    chunk=Monotable::MemoryChunk.new
    data=load_test_data "0-255.binary"

    chunk["test_key"]= {"test_column"=>data}
    chunk_str=chunk.to_binary

    chunk2=Monotable::MemoryChunk.new(:data=>chunk_str)
    chunk2["test_key"]["test_column"].should == data
  end

  def add_to_chunk(chunk,prefix,num_records)
    num_records.times do |i|
      k="#{prefix}#{i}"
      chunk[k]={"data"=>k}
    end
  end

  def test_chunk(num_records=2)
    chunk=Monotable::MemoryChunk.new
    add_to_chunk(chunk,"key",num_records)
    chunk
  end

  it "should work to get_first :gte" do
    result=test_chunk(5).get_first(:gte=>"key2")
    result.collect{|a|a[0]}.should == ["key2"]
  end

  it "should work to get_first :gt" do
    result=test_chunk(5).get_first(:gt=>"key2")
    result.collect{|a|a[0]}.should == ["key3"]
  end

  it "should work to get_first :with_prefix" do
    chunk=test_chunk(5)
    add_to_chunk(chunk,"apple",3)
    add_to_chunk(chunk,"legos",3)
    add_to_chunk(chunk,"zoo",3)

    result=chunk.get_first(:with_prefix=>"legos", :limit=>2)
    result.collect{|a|a[0]}.should == ["legos0","legos1"]
  end

  it "should work to get_first with limits" do
    chunk=test_chunk(5)
    result=chunk.get_first(:gte=>"key2", :limit=>2)
    result.collect{|a|a[0]}.should == ["key2","key3"]

    result=chunk.get_first(:gte=>"key2", :limit=>3)
    result.collect{|a|a[0]}.should == ["key2","key3","key4"]

    result=chunk.get_first(:gte=>"key2", :limit=>4)
    result.collect{|a|a[0]}.should == ["key2","key3","key4"]
  end

  it "should work to get_last :lte" do
    result=test_chunk(5).get_last(:lte=>"key2")
    result.collect{|a|a[0]}.should == ["key2"]
  end

  it "should work to get_last :lt" do
    result=test_chunk(5).get_last(:lt=>"key2")
    result.collect{|a|a[0]}.should == ["key1"]
  end

  it "should work to get_last :lte, :gte" do
    result=test_chunk(5).get_last(:gte => "key1", :lte=>"key3", :limit=>10)
    result.collect{|a|a[0]}.should == ["key1","key2","key3"]
  end

  it "should work to get_last :lte, :gte, :limit=>2" do
    result=test_chunk(5).get_last(:gte => "key1", :lte=>"key3", :limit=>2)
    result.collect{|a|a[0]}.should == ["key2","key3"]
  end

  it "should work to get_first :lte, :gte" do
    result=test_chunk(5).get_first(:gte => "key1", :lte=>"key3", :limit=>10)
    result.collect{|a|a[0]}.should == ["key1","key2","key3"]
  end

  it "should work to get_first :lte, :gte, :limit=>2" do
    result=test_chunk(5).get_first(:gte => "key1", :lte=>"key3", :limit=>2)
    result.collect{|a|a[0]}.should == ["key1","key2"]
  end

  it "should work to get_last with limits" do
    chunk=test_chunk(5)
    result=chunk.get_last(:lte=>"key2",:limit => 2)
    result.collect{|a|a[0]}.should == ["key1","key2"]

    result=chunk.get_last(:lte=>"key2",:limit => 3)
    result.collect{|a|a[0]}.should == ["key0","key1","key2"]

    result=chunk.get_last(:lte=>"key2",:limit => 4)
    result.collect{|a|a[0]}.should == ["key0","key1","key2"]
  end

  it "should work to get_last :with_prefix" do
    chunk=test_chunk(5)
    add_to_chunk(chunk,"apple",3)
    add_to_chunk(chunk,"legos",3)
    add_to_chunk(chunk,"zoo",3)

    result=chunk.get_last(:with_prefix=>"legos", :limit=>2)
    result.collect{|a|a[0]}.should == ["legos1","legos2"]
  end

end
