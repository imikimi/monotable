require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"../lib/monotable/solo_daemon/solo_daemon.rb")

describe Monotable::DiskChunk do
  include MonotableHelperMethods

  it "should be possible to initialize a new SoloDaemon" do
    reset_temp_dir
    solo=Monotable::SoloDaemon.new(:store_paths=>[temp_dir],:initialize_new_store=>true)
    solo.chunks.length.should == 1
  end

  it "should auto-split too-big chunks" do
    reset_temp_dir
    solo=Monotable::SoloDaemon.new(:store_paths=>[temp_dir],:initialize_new_store=>true)

    # set a small max_chunk_size for testing
    solo.chunks.each {|key,chunk| chunk.max_chunk_size=4000}

    data="a"*1024

    solo.set("a","data"=>data)
    solo.set("b","data"=>data)
    solo.set("c","data"=>data)
    solo.chunks.length.should == 1

    solo.set("d","data"=>data)
    solo.chunks.length.should == 2
    solo.verify_chunk_ranges
    cs=solo.chunks.values
    cs[0].range_end.should == cs[1].range_start
    (cs[0].range_end && true).should==true
    (cs[1].range_start && true).should==true
  end

  it "should auto-split too-big journals" do
    reset_temp_dir
    solo=Monotable::SoloDaemon.new(:store_paths=>[temp_dir],:initialize_new_store=>true)

    # set a small max_journal_size for testing
    start_journal=solo.path_stores[0].journal_manager.current_journal
    start_journal.max_journal_size=4000

    data="a"*1024

    solo.set("a","data"=>data)
    solo.set("b","data"=>data)

    # just before overflow
    solo.set("c","data"=>data)
    solo.path_stores[0].journal_manager.current_journal.should==start_journal
    solo.path_stores[0].journal_manager.current_journal.size.should_not==0

    # journal overflow
    solo.set("d","data"=>data)
    solo.path_stores[0].journal_manager.current_journal.should_not==start_journal
    solo.path_stores[0].journal_manager.current_journal.size.should==0

    # write to new journal
    solo.set("e","data"=>data)
    solo.path_stores[0].journal_manager.current_journal.size.should_not==0
  end

  it "should auto-split too-big chunks and auto-compact too-big journals" do
    reset_temp_dir
    solo=Monotable::SoloDaemon.new(:store_paths=>[temp_dir],:max_chunk_size => 4000,:initialize_new_store=>true)

    # verify small max_chunk_size is propagated
    solo.chunks.each {|key,chunk| chunk.max_chunk_size.should == 4000}

    # set a small max_journal_size for testing
    start_journal=solo.path_stores[0].journal_manager.current_journal
    start_journal.max_journal_size=6000

    data="a"*1024

    solo.set("a","data"=>data)
    solo.set("b","data"=>data)
    solo.set("c","data"=>data)
    solo.chunks.length.should == 1

    solo.set("d","data"=>data)
    solo.chunks.length.should == 2

    chunks=solo.chunks.values
    chunks.collect {|c| c.range}.should == [["","c"],["c",:infinity]]
    chunks.each {|c| c.verify_accounting_size}

    solo.set("e","data"=>data)
    chunks.each {|c| c.verify_accounting_size}
    solo.set("f","data"=>data)
  end

  def test_index_block_structure(max_chunk_size, max_index_block_size, num_records, data_multiplier, expected_chunk_count, expected_depth)
    Monotable::Global.reset
    reset_temp_dir
    options={:store_paths=>[temp_dir],:max_chunk_size => max_chunk_size, :max_index_block_size => max_index_block_size}
    solo = Monotable::SoloDaemon.new(options.merge(:initialize_new_store=>true))

    control_set={}
    (1..num_records).each do |n|
      key = "%010d" % n
      record = {"data" => key*data_multiplier}
      control_set[key] = record
      solo.set(key, record)
    end
    solo.compact

    solo2 = Monotable::SoloDaemon.new(options)

    solo2.chunks.length.should == expected_chunk_count
    chunk = solo2.get_chunk("")
    chunk.index_level_offsets.length.should == expected_depth

    solo2.length.should == control_set.length
    control_set.each do |key,record|
      solo2.get(key).should == record
    end
    [solo2,control_set]
  end

  it "should work to have 25 chunks" do test_index_block_structure(1024,256,128,10,25,1) end
  it "should work to have 1 index level" do test_index_block_structure(64*1024,64,8,2,1,1) end
  it "should work to have 2 index level" do test_index_block_structure(64*1024,64,32,2,1,2) end
  it "should work to have 3 index level" do test_index_block_structure(64*1024,64,128,2,1,3) end
  it "should work to have 4 index level" do test_index_block_structure(64*1024,64,512,2,1,4) end

  it "should work to create a multi-store" do
    reset_temp_dir
    solo=Monotable::SoloDaemon.new(:store_paths=>[temp_dir])
    solo.initialize_new_multi_store
  end
end
