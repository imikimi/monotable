require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe Monotable::LocalStore do
  include MonotableHelperMethods

  def setup_store
    reset_temp_dir
    Monotable::LocalStore.new(:store_paths=>[temp_dir],:initialize_new_store=>true)
  end

  it "should be possible to initialize a new LocalStore" do
    local_store=setup_store
    local_store.chunks.length.should == 1
  end

  it "should be possible to init from an existing LocalStore" do
    local_store=setup_store

    # open existing localstore
    local_store=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    local_store.chunks.length.should == 1

    local_store.chunks.each do |k,v|
      v.kind_of?(Monotable::DiskChunk).should == true
    end
  end

  it "should be possible to add entries to the localstore" do
    local_store=setup_store

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
    local_store=setup_store
    load_test_data_directory(local_store)
    local_store.get_chunk("").journal.compact

    #load LocalStore anew
    local_store2=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    local_store2.chunks.length.should == 1
    local_store2.get_chunk("").length.should == 4
    Monotable::MemoryChunk.load(local_store2.get_chunk("").filename).accounting_size.should == 15994
  end

  it "should be possible to attach a localstore to a path with a non-compacted journal" do
    local_store=setup_store
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
    local_store=setup_store

    #load-it-up
    load_test_data_directory(local_store)

    # compact it
    local_store.path_stores[0].journal_manager.current_journal.compact

    # load the chunk
    chunk=Monotable::MemoryChunk.load(local_store.get_chunk("").filename)
    chunk.keys.sort.should == ["0-255.binary", "declaration_of_independence.txt", "plato.jpeg", "simple.png"].sort
  end

  it "should be possible to split a chunk" do
    local_store=setup_store

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
    local_store=setup_store

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
      :max_index_block_size => test_max_index_block_size,
      :initialize_new_store => true
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

  it "should not return the record after we've deleted it" do
    local_store=setup_store

    record_key = 'apple'
    record_value = { 'x' => '1' }
    local_store.set(record_key,record_value)

    local_store.get(record_key).should_not==nil

    local_store.delete(record_key)
    local_store.get(record_key).should==nil
  end

  #*******************************************************
  # test get_first and get_last
  #*******************************************************

  it "should work to get_first :gte" do
    result=setup_store_with_test_keys.get_first(:gte=>"key2")
    result[:records].collect{|a|a[0]}.should == ["key2"]
  end

  it "should work to get_first :gt" do
    result=setup_store_with_test_keys.get_first(:gt=>"key2")
    result[:records].collect{|a|a[0]}.should == ["key3"]
  end

  it "should work to get_first :with_prefix" do
    chunk=setup_store_with_test_keys
    add_test_keys(chunk,"apple",3)
    add_test_keys(chunk,"legos",3)
    add_test_keys(chunk,"zoo",3)

    result=chunk.get_first(:with_prefix=>"legos", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["legos0","legos1"]
  end

  it "should work to get_first with limits" do
    chunk=setup_store_with_test_keys
    result=chunk.get_first(:gte=>"key2", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["key2","key3"]

    result=chunk.get_first(:gte=>"key2", :limit=>3)
    result[:records].collect{|a|a[0]}.should == ["key2","key3","key4"]

    result=chunk.get_first(:gte=>"key2", :limit=>4)
    result[:records].collect{|a|a[0]}.should == ["key2","key3","key4"]
  end

  it "should work to get_last :lte" do
    result=setup_store_with_test_keys.get_last(:lte=>"key2")
    result[:records].collect{|a|a[0]}.should == ["key2"]
  end

  it "should work to get_last :lt" do
    result=setup_store_with_test_keys.get_last(:lt=>"key2")
    result[:records].collect{|a|a[0]}.should == ["key1"]
  end

  it "should work to get_last :lte, :gte" do
    result=setup_store_with_test_keys.get_last(:gte => "key1", :lte=>"key3", :limit=>10)
    result[:records].collect{|a|a[0]}.should == ["key1","key2","key3"]
  end

  it "should work to get_last :lte, :gte, :limit=>2" do
    result=setup_store_with_test_keys.get_last(:gte => "key1", :lte=>"key3", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["key2","key3"]
  end

  it "should work to get_first :lte, :gte" do
    result=setup_store_with_test_keys.get_first(:gte => "key1", :lte=>"key3", :limit=>10)
    result[:records].collect{|a|a[0]}.should == ["key1","key2","key3"]
  end

  it "should work to get_first :lte, :gte, :limit=>2" do
    result=setup_store_with_test_keys.get_first(:gte => "key1", :lte=>"key3", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["key1","key2"]
  end

  it "should work to get_last with limits" do
    chunk=setup_store_with_test_keys
    result=chunk.get_last(:lte=>"key2",:limit => 2)
    result[:records].collect{|a|a[0]}.should == ["key1","key2"]

    result=chunk.get_last(:lte=>"key2",:limit => 3)
    result[:records].collect{|a|a[0]}.should == ["key0","key1","key2"]

    result=chunk.get_last(:lte=>"key2",:limit => 4)
    result[:records].collect{|a|a[0]}.should == ["key0","key1","key2"]
  end

  it "should work to get_last :with_prefix" do
    chunk=setup_store_with_test_keys
    add_test_keys(chunk,"apple",3)
    add_test_keys(chunk,"legos",3)
    add_test_keys(chunk,"zoo",3)

    result=chunk.get_last(:with_prefix=>"legos", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["legos1","legos2"]
  end
end
