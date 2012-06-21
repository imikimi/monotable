require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")

describe Monotable::MemoryChunk do
  include MonotableHelperMethods

  def blank_store
    Monotable::MemoryChunk.new
  end

  it_should_behave_like "monotable api"

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

  it "should serialize with binary column data" do
    store=blank_store
    data=load_test_data "0-255.binary"

    store["test_key"]= {"test_column"=>data}
    chunk_str=store.to_binary

    chunk2=Monotable::MemoryChunk.new(:data=>chunk_str)
    chunk2["test_key"]["test_column"].should == data
  end
end
