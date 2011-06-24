require File.join(File.dirname(__FILE__),"../lib/monotable/monotable")
require File.join(File.dirname(__FILE__),"../lib/monotable/solo_daemon/solo_daemon.rb")
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe MonoTable::ChunkFile do
  include MonoTableHelperMethods

  it "should be possible to initialize a new SoloDaemon" do
    reset_temp_dir
    solo=MonoTable::SoloDaemon.new(temp_dir)
    solo.chunks.length.should == 1
  end

  it "should auto-split too-big chunks" do
    reset_temp_dir
    solo=MonoTable::SoloDaemon.new(temp_dir)

    # set a small max_chunk_size for testing
    solo.chunks.each {|key,chunk| chunk.max_chunk_size=4000}

    data="a"*1024

    solo.set("a",:data=>data)
    solo.set("b",:data=>data)
    solo.set("c",:data=>data)
    solo.chunks.length.should == 1

    solo.set("d",:data=>data)
    solo.chunks.length.should == 2
    solo.verify_chunk_ranges
    cs=solo.chunks.values
    cs[0].range_end.should == cs[1].range_start
    (cs[0].range_end && true).should==true
    (cs[1].range_start && true).should==true
  end

  it "should auto-split too-big journals" do
    reset_temp_dir
    solo=MonoTable::SoloDaemon.new(temp_dir)

    # set a small max_journal_size for testing
    start_journal=solo.path_stores[0].journal_manager.current_journal
    start_journal.max_journal_size=4000

    data="a"*1024

    solo.set("a",:data=>data)
    solo.set("b",:data=>data)

    # just before overflow
    solo.set("c",:data=>data)
    solo.path_stores[0].journal_manager.current_journal.should==start_journal
    solo.path_stores[0].journal_manager.current_journal.size.should_not==0

    # journal overflow
    solo.set("d",:data=>data)
    solo.path_stores[0].journal_manager.current_journal.should_not==start_journal
    solo.path_stores[0].journal_manager.current_journal.size.should==0

    # write to new journal
    solo.set("e",:data=>data)
    solo.path_stores[0].journal_manager.current_journal.size.should_not==0
  end

  it "should auto-split too-big chunks and auto-compact too-big journals" do
    reset_temp_dir
    solo=MonoTable::SoloDaemon.new(temp_dir)

    # set a small max_chunk_size for testing
    solo.chunks.each {|key,chunk| chunk.max_chunk_size=4000}

    # set a small max_journal_size for testing
    start_journal=solo.path_stores[0].journal_manager.current_journal
    start_journal.max_journal_size=6000

    data="a"*1024

    solo.set("a",:data=>data)
    solo.set("b",:data=>data)
    solo.set("c",:data=>data)
    solo.chunks.length.should == 1

    solo.set("d",:data=>data)
    solo.chunks.length.should == 2

    chunks=solo.chunks.values
    chunks.collect {|c| c.range}.should == [["","c"],["c",:infinity]]
    chunks.each {|c| c.verify_accounting_size}

    solo.set("e",:data=>data)
    chunks.each {|c| c.verify_accounting_size}
    solo.set("f",:data=>data)
  end
end
