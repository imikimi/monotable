require File.join(File.dirname(__FILE__),"../lib/monotable/monotable")
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe MonoTable::DiskChunk do
  include MonoTableHelperMethods

  it "should be possible to read chunkified directory" do
    file=chunkify_test_data_directory

    chunk = MonoTable::DiskChunk.new(file)
  end

  it "should be possible to list chunkified directory" do
    file=chunkify_test_data_directory

    chunk = MonoTable::DiskChunk.new(file)
    chunk.keys.sort.should == %w{0-255.binary declaration_of_independence.txt plato.jpeg simple.png}
  end

  it "should return nil if key is not in chunk" do
    file=chunkify_test_data_directory

    chunk = MonoTable::DiskChunk.new(file)
    chunk["not_there"].should == nil
  end

  def test_read(chunk,filename)
    data=chunk[filename]
    data.should_not == nil
    data["file_data"].should == load_test_data(filename)
  end

  it "should be possible to extract values from chunkified directory" do
    file=chunkify_test_data_directory

    chunk = MonoTable::DiskChunk.new(file)

    test_read(chunk,"declaration_of_independence.txt")
    test_read(chunk,"plato.jpeg")
    test_read(chunk,"0-255.binary")
  end

  it "should work to journal a 'set' to a DiskChunk" do
    file=chunkify_test_data_directory

    chunkfile = MonoTable::DiskChunk.new(file)

    chunkfile.set("foo",{"bar" => "June"})

    # test get after set with DiskChunk
    chunkfile.get("foo").should =={"bar" => "June"}
  end

  it "should work to 'get' using MonoTable::MemoryChunk from a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = MonoTable::DiskChunk.new(file)

    chunkfile.set("foo",{"bar" => "June"})
    chunkfile.journal.compact

    # test full loading with MemoryChunk
    chunk = MonoTable::MemoryChunk.load(file)
    chunk.keys.sort.should == ["0-255.binary", "declaration_of_independence.txt", "plato.jpeg", "simple.png", "foo"].sort
    chunk["foo"].should == {"bar" => "June"}
  end

  it "should work to 'get' after 'set' using MonoTable::DiskChunk from a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = MonoTable::DiskChunk.new(file)

    chunkfile.set("foo",{"bar" => "June"})
    chunkfile.get("foo").should == {"bar" => "June"}

    # compact the chunkfile
    chunkfile.journal.compact

    # test partial loading with DiskChunk
    chunkfile2 = MonoTable::DiskChunk.new(file)
    chunkfile2.get("foo").should =={"bar" => "June"}
  end

  it "should work to 'delete' using MonoTable::MemoryChunk and a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = MonoTable::DiskChunk.new(file)

    chunkfile.delete("declaration_of_independence.txt")
    chunkfile.journal.compact

    # test partial loading with DiskChunk
    chunkfile2 = MonoTable::MemoryChunk.load(file)
    chunkfile2.get("declaration_of_independence.txt").should == nil
  end

  it "should work to 'delete' using MonoTable::DiskChunk and a journaled entry" do
    file=chunkify_test_data_directory

    chunkfile = MonoTable::DiskChunk.new(file)

    chunkfile.get("declaration_of_independence.txt")["file_data"].length.should == 407
    chunkfile.delete("declaration_of_independence.txt")
    chunkfile.get("declaration_of_independence.txt").should == nil

    # compact the chunkfile
    chunkfile.journal.compact

    # test partial loading with DiskChunk
    chunkfile2 = MonoTable::DiskChunk.new(file)
    chunkfile2.get("declaration_of_independence.txt").should == nil
  end

  it "should work to journal an 'update' to a DiskChunk" do
    file=chunkify_test_data_directory

    chunkfile = MonoTable::DiskChunk.new(file)

    chunkfile.set("foo",{"bar" => "June", "baz" => "December"})
    chunkfile.journal.compact

    chunk = MonoTable::MemoryChunk.load(file)
    chunk["foo"].should == {"bar" => "June", "baz" => "December"}

    chunkfile = MonoTable::DiskChunk.new(file)
    chunkfile.verify_records
    chunkfile.update("foo",{"baz" => "January"})
    chunkfile.journal.compact

    chunk = MonoTable::MemoryChunk.load(file)
    chunk.keys.sort.should == ["0-255.binary", "declaration_of_independence.txt", "plato.jpeg", "simple.png", "foo"].sort
    chunk["foo"].should == {"bar" => "June", "baz" => "January"}
  end

  it "should work to journal a 'delete' to a DiskChunk" do
    file=chunkify_test_data_directory

    chunkfile = MonoTable::DiskChunk.new(file)

    chunkfile.delete("plato.jpeg")
    chunkfile.journal.compact

    chunk = MonoTable::MemoryChunk.load(file)

    chunk.keys.sort.should == ["0-255.binary", "declaration_of_independence.txt", "simple.png"].sort
    chunk["plato.jpeg"].should == nil
  end

end
