require File.join(File.dirname(__FILE__),"../lib/monotable/monotable")
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe Monotable::LocalStore do
  include MonotableHelperMethods

  it "should be possible to initialize a new LocalStore" do
    reset_temp_dir
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    local_store.chunks.length.should == 1
  end

  it "should be possible to init from an existing LocalStore" do
    reset_temp_dir
    #init new localstore
    Monotable::LocalStore.new(:store_paths=>[temp_dir])

    # open existing localstore
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    local_store.chunks.length.should == 1

    local_store.chunks.each do |k,v|
      v.kind_of?(Monotable::DiskChunk).should == true
    end
  end

  it "should be possible to add entries to the localstore" do
    reset_temp_dir
    #init new localstore
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])

    #load-it-up
    local_store.chunks.length.should == 1
    local_store.get_chunk("").accounting_size.should == 0
    load_test_data_directory(local_store)
    local_store.chunks.length.should == 1
    local_store.get_chunk("").records.length.should == 4
    local_store.get_chunk("").verify_accounting_size
    local_store.get_chunk("").accounting_size.should == 15994
  end

  it "should be possible to attach a localstore to a path with existing data" do
    reset_temp_dir
    #init new localstore
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    load_test_data_directory(local_store)
    local_store.get_chunk("").journal.compact

    #load LocalStore anew
    local_store2=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    local_store2.chunks.length.should == 1
    local_store2.get_chunk("").length.should == 4
    Monotable::MemoryChunk.load(local_store2.get_chunk("").filename).accounting_size.should == 15994
  end

  it "should be possible to attach a localstore to a path with a non-compacted journal" do
    reset_temp_dir
    #init new localstore
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    load_test_data_directory(local_store)

    #load LocalStore anew
    local_store2=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    local_store2.chunks.length.should == 1
    local_store2.get_chunk("").length.should == 4
    Monotable::MemoryChunk.load(local_store2.get_chunk("").filename).accounting_size.should == 15994

    #test write
    local_store2.set("testkey",{"field"=>"value"})
  end

  it "should be possible to compact a journal" do
    reset_temp_dir
    #init new localstore
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])

    #load-it-up
    load_test_data_directory(local_store)

    # compact it
    local_store.path_stores[0].journal_manager.current_journal.compact

    # load the chunk
    chunk=Monotable::MemoryChunk.load(local_store.get_chunk("").filename)
    chunk.keys.sort.should == ["0-255.binary", "declaration_of_independence.txt", "plato.jpeg", "simple.png"].sort
  end

  it "should be possible to split a chunk" do
    reset_temp_dir
    #init new localstore
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])

    #load-it-up
    load_test_data_directory(local_store)

    # split the chunk
    chunk1=local_store.get_chunk("")
    chunk1.length.should==4
    chunk2=chunk1.split("declaration_of_independence.txt")
    chunk1.length.should==1
    chunk2.length.should==3
  end

  it "should be possible to split a chunk on a specific key" do
    reset_temp_dir
    #init new localstore
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])

    #load-it-up
    load_test_data_directory(local_store)

    # split the chunk
    chunk1=local_store.get_chunk("")
    chunk1.length.should==4
    chunk2=chunk1.split
    chunk1.length.should==3
    chunk2.length.should==1
  end

  it "max_chunk_size and max_index_block_size should propagate" do
    reset_temp_dir
    test_max_chunk_size = 16 * 1024
    test_max_index_block_size = 256
    local_store=Monotable::LocalStore.new(
      :store_paths => [temp_dir],
      :max_chunk_size => test_max_chunk_size,
      :max_index_block_size => test_max_index_block_size
    )

    # split the chunk
    path_store1 = local_store.path_stores[0]
    path_store1.max_chunk_size.should == test_max_chunk_size
    path_store1.max_index_block_size.should == test_max_index_block_size

    # split the chunk
    chunk1 = local_store.get_chunk("")
    chunk1.max_chunk_size.should == test_max_chunk_size
    chunk1.max_index_block_size.should == test_max_index_block_size
  end
end
