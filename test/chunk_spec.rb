require File.join(File.dirname(__FILE__),"../lib/monotable/monotable")
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe MonoTable::MemoryChunk do
  include MonoTableHelperMethods

  it "should be possible to create an in-memory chunk from scratch" do
    chunk=MonoTable::MemoryChunk.new
    chunk.should_not == nil
  end

  it "should be possible to serialize and deserialize a chunk with one record, one column" do
    chunk=MonoTable::MemoryChunk.new
    chunk["test_key"]= {"test_column"=>"test_value"}
    chunk_str=chunk.to_binary

    chunk2=MonoTable::MemoryChunk.new(chunk_str)
    chunk2.length.should == 1
    chunk2["test_key"].should == {"test_column"=>"test_value"}
  end

  it "should return nil if key is not in chunk" do
    chunk=MonoTable::MemoryChunk.new
    chunk["test_key"]= {"test_column"=>"test_value"}
    chunk_str=chunk.to_binary

    chunk2=MonoTable::MemoryChunk.new(chunk_str)
    chunk2["test_key2"].should == nil
  end

  it "should be possible to de/serialize a chunk with multiple records" do
    chunk=MonoTable::MemoryChunk.new
    chunk["key1"]={"col1"=>"val1","col2"=>"val2"}
    chunk["key2"]={"col2"=>"val3","col3"=>"val4"}
    chunk_str=chunk.to_binary

    chunk2=MonoTable::MemoryChunk.new(chunk_str)
    chunk2.length.should == 2
    chunk2["key1"].should == {"col1"=>"val1","col2"=>"val2"}
    chunk2["key2"].should == {"col2"=>"val3","col3"=>"val4"}
  end

  it "should be possible to store binary data in a chunk column" do
    chunk=MonoTable::MemoryChunk.new
    data=load_test_data "0-255.binary"

    chunk["test_key"]= {"test_column"=>data}
    chunk_str=chunk.to_binary

    chunk2=MonoTable::MemoryChunk.new(chunk_str)
    chunk2["test_key"]["test_column"].should == data
  end
end
