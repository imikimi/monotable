require File.join(File.dirname(__FILE__),"../lib/monotable/monotable")
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe Monotable::DiskChunk do
  include MonotableHelperMethods

  it "should be possible to read chunkified directory" do
    file=chunkify_test_data_directory

    chunk = Monotable::DiskChunk.new(:filename=>file)
  end

  it "should be possible to list chunkified directory" do
    file=chunkify_test_data_directory

    chunk = Monotable::DiskChunk.new(:filename=>file)
    chunk.keys.sort.should == %w{0-255.binary declaration_of_independence.txt plato.jpeg simple.png}
  end

  it "should return nil if key is not in chunk" do
    file=chunkify_test_data_directory

    chunk = Monotable::DiskChunk.new(:filename=>file)
    chunk["not_there"].should == nil
  end

  def test_read(chunk,filename)
    data=chunk[filename]
    data.should_not == nil
    data["file_data"].should == load_test_data(filename)
  end

  it "should be possible to extract values from chunkified directory" do
    file=chunkify_test_data_directory

    chunk = Monotable::DiskChunk.new(:filename=>file)

    test_read(chunk,"declaration_of_independence.txt")
    test_read(chunk,"plato.jpeg")
    test_read(chunk,"0-255.binary")
  end

  it "should work to journal a 'set' to a DiskChunk" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.set("foo",{"bar" => "June"})

    # test get after set with DiskChunk
    chunkfile.get("foo").should =={"bar" => "June"}
  end

  it "should work to 'get' using Monotable::MemoryChunk from a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.set("foo",{"bar" => "June"})
    chunkfile.journal.compact

    # test full loading with MemoryChunk
    chunk = Monotable::MemoryChunk.load(file)
    chunk.keys.sort.should == ["0-255.binary", "declaration_of_independence.txt", "plato.jpeg", "simple.png", "foo"].sort
    chunk["foo"].should == {"bar" => "June"}
  end

  it "should work to 'get' after 'set' using Monotable::DiskChunk from a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.set("foo",{"bar" => "June"})
    chunkfile.get("foo").should == {"bar" => "June"}

    # compact the chunkfile
    chunkfile.journal.compact

    # test partial loading with DiskChunk
    chunkfile2 = Monotable::DiskChunk.new(:filename=>file)
    chunkfile2.get("foo").should =={"bar" => "June"}
  end

  it "should work to 'delete' using Monotable::MemoryChunk and a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.delete("declaration_of_independence.txt")
    chunkfile.journal.compact

    # test partial loading with DiskChunk
    chunkfile2 = Monotable::MemoryChunk.load(file)
    chunkfile2.get("declaration_of_independence.txt").should == nil
  end

  it "should work to 'delete' using Monotable::DiskChunk and a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.get("declaration_of_independence.txt")["file_data"].length.should == 407
    chunkfile.delete("declaration_of_independence.txt")
    chunkfile.get("declaration_of_independence.txt").should == nil

    # compact the chunkfile
    chunkfile.journal.compact

    # test partial loading with DiskChunk
    chunkfile2 = Monotable::DiskChunk.new(:filename=>file)
    chunkfile2.get("declaration_of_independence.txt").should == nil
  end

  it "should work to journal an 'update' to a DiskChunk" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.set("foo",{"bar" => "June", "baz" => "December"})
    chunkfile.journal.compact

    chunk = Monotable::MemoryChunk.load(file)
    chunk["foo"].should == {"bar" => "June", "baz" => "December"}

    chunkfile = Monotable::DiskChunk.new(:filename=>file)
    chunkfile.verify_records
    chunkfile.update("foo",{"baz" => "January"})
    chunkfile.journal.compact

    chunk = Monotable::MemoryChunk.load(file)
    chunk.keys.sort.should == ["0-255.binary", "declaration_of_independence.txt", "plato.jpeg", "simple.png", "foo"].sort
    chunk["foo"].should == {"bar" => "June", "baz" => "January"}
  end

  it "should work to journal a 'delete' to a DiskChunk" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.delete("plato.jpeg")
    chunkfile.journal.compact

    chunk = Monotable::MemoryChunk.load(file)

    chunk.keys.sort.should == ["0-255.binary", "declaration_of_independence.txt", "simple.png"].sort
    chunk["plato.jpeg"].should == nil
  end

  it "should work to get_first" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    result=chunkfile.get_first(:gte=>"plato.jpeg", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["plato.jpeg","simple.png"]
  end

  def setup_store
    reset_temp_dir
    Monotable::DiskChunk.new(:filename=>File.join(temp_dir,"test#{Monotable::CHUNK_EXT}"))
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
