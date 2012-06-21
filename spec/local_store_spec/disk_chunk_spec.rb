require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")

describe Monotable::DiskChunk do
  include MonotableHelperMethods
  it_should_behave_like "monotable api"

  #***************************************
  # helpers
  #***************************************
  def blank_store
    reset_temp_dir
    filename=File.join(temp_dir,"test#{Monotable::CHUNK_EXT}")
    Monotable::MemoryChunk.new().save(filename)
    Monotable::DiskChunk.init(:filename=>filename)
  end

  #***************************************
  # tests
  #***************************************
  it "should be possible to read chunkified directory" do
    file=chunkify_test_data_directory

    chunk = Monotable::DiskChunk.new(:filename=>file)
  end

  it "should be possible to list chunkified directory" do
    file=chunkify_test_data_directory

    chunk = Monotable::DiskChunk.new(:filename=>file)
    chunk.keys.sort.should == %w{0-255.binary 0-65535.words.binary declaration_of_independence.txt plato.jpeg simple.png}
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
    chunkfile["foo"].should == {"bar"=>"June"}
  end

  it "should work to 'get' using Monotable::MemoryChunk from a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.set("foo",{"bar" => "June"})
    chunkfile.journal.compact

    # test full loading with MemoryChunk
    chunk = Monotable::MemoryChunk.load(file)
    chunk.keys.sort.should == ["0-255.binary", "0-65535.words.binary", "declaration_of_independence.txt", "plato.jpeg", "simple.png", "foo"].sort
    chunk["foo"].should == {"bar" => "June"}
  end

  it "should work to 'get' after 'set' using Monotable::DiskChunk from a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.set("foo",{"bar" => "June"})
    chunkfile["foo"].should == {"bar" => "June"}

    # compact the chunkfile
    chunkfile.journal.compact

    # test partial loading with DiskChunk
    chunkfile2 = Monotable::DiskChunk.new(:filename=>file)
    chunkfile2["foo"].should =={"bar" => "June"}
  end

  it "should work to 'delete' using Monotable::MemoryChunk and a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.delete("declaration_of_independence.txt")
    chunkfile.journal.compact

    # test partial loading with DiskChunk
    chunkfile2 = Monotable::MemoryChunk.load(file)
    chunkfile2["declaration_of_independence.txt"].should == nil
  end

  it "should work to 'delete' using Monotable::DiskChunk and a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile["declaration_of_independence.txt"]["file_data"].length.should == 407
    chunkfile.delete("declaration_of_independence.txt")
    chunkfile["declaration_of_independence.txt"].should == nil

    # compact the chunkfile
    chunkfile.journal.compact

    # test partial loading with DiskChunk
    chunkfile2 = Monotable::DiskChunk.new(:filename=>file)
    chunkfile2["declaration_of_independence.txt"].should == nil
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
    chunk.keys.sort.should == ["0-255.binary", "0-65535.words.binary", "declaration_of_independence.txt", "plato.jpeg", "simple.png", "foo"].sort
    chunk["foo"].should == {"bar" => "June", "baz" => "January"}
  end

  it "should work to journal a 'delete' to a DiskChunk" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.delete("plato.jpeg")
    chunkfile.journal.compact

    chunk = Monotable::MemoryChunk.load(file)

    chunk.keys.sort.should == ["0-255.binary", "0-65535.words.binary", "declaration_of_independence.txt", "simple.png"].sort
    chunk["plato.jpeg"].should == nil
  end

  it "each_key should return valid keys matched to record" do
    file=chunkify_test_data_directory

    chunkfile = Monotable::DiskChunk.new(:filename=>file)

    chunkfile.each_key do |k|
      record=chunkfile.get_record(k)
      record.should_not == nil
    end
  end

  it "each_key should work and records should be fetchable if there data is in the journal" do
    result=setup_store_with_test_keys(5)
    result.each_key do |k|
      record=result.get_record(k)
      record.class.should==Monotable::JournalDiskRecord
      record.should_not == nil
    end
  end

  it "each_key should work and records should be fetchable if their data is in the chunkfile" do
    chunk_store=setup_store_with_test_keys(5)
    chunk_store.journal.compact
    chunk_store.reset
    chunk_store.each_key do |k|
      record=chunk_store.get_record(k)
      record.class.should==Monotable::DiskRecord
      record.keys.should==["data"]
      record.should_not == nil
    end
  end

end
