require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"common_api_tests")

# TODO: test the case where we write to a chunk, start async compaction, write again while compaction is going, and
# THEN compaction completes. Are we in a consistent state? I don't think the chunk.reset currently does the right thing.

describe Monotable::Journal do
  include MonotableHelperMethods

  def blank_store(options={})
    reset_temp_dir
    Monotable::LocalStore.new({:store_paths=>[temp_dir],:initialize_new_test_store=>true}.merge(options))
  end

  def basic_server(options={})
    reset_temp_dir
    Monotable::Server.new({:store_paths=>[temp_dir],:initialize_new_store=>true}.merge(options))
  end

  before :each do
    Monotable::Journal.async_compaction=false
  end
  after :each do
    Monotable::Journal.async_compaction=false
  end

  it "async journal compaction should work" do
    local_store=blank_store
    load_test_data_directory(local_store)
    Monotable::Journal.async_compaction=true
    EventMachine::run do
      local_store.path_stores[0].compact do
        EM::stop
      end
    end
    #load LocalStore anew
    local_store2=Monotable::LocalStore.new(:store_paths=>[temp_dir])
    local_store2.chunks.length.should == 1
    local_store2.get_chunk("").length.should == 5
    Monotable::MemoryChunk.load(local_store2.get_chunk("").filename).accounting_size.should == 147095
  end

  it "compact after init" do
    server = basic_server
    server.local_store.chunks.keys.should == ["", "+++0", "++0", "+0", "0"]
    server.local_store.chunks[""].keys.should == ["++++0"]
    server.local_store.compact
    server.local_store.chunks.keys.should == ["", "+++0", "++0", "+0", "0"]
    server.local_store.chunks[""].keys.should == ["++++0"]
  end

  def run_1_chunk_auto_split
    local_store=blank_store(:max_chunk_size => 10)
    EventMachine::run do
    EM.next_tick do
    local_store["a"] = {"foo" => "bar1"}
    EM.next_tick do
    local_store["b"] = {"foo" => "bar2"}
    EM.next_tick do
    local_store.path_stores[0].compact do
      EM::stop
    end
    end
    end
    end
    end
    #load LocalStore anew
    local_store2 = Monotable::LocalStore.new(:store_paths=>[temp_dir])
    local_store2.chunks.keys.should == ["","b"]
    chunk1 = local_store2.get_chunk("")
    chunk2 = local_store2.get_chunk("b")
    chunk1.range_start.should == ""
    chunk1.range_end.should == chunk2.range_start
    chunk2.range_end.should == Monotable::LAST_POSSIBLE_KEY
    chunk1.length.should == 1
    chunk2.length.should == 1
    local_store2["a"].should == {"foo" => "bar1"}
    local_store2["b"].should == {"foo" => "bar2"}
  end

  it "1 chunk should automatically split in an evented fashion (synchronous compaction)" do
    run_1_chunk_auto_split
  end

  it "1 chunk should automatically split in an evented fashion (asynchronous compaction)" do
    Monotable::Journal.async_compaction = true
    run_1_chunk_auto_split
  end

  def run_2_chunk_auto_split
    local_store=blank_store(:max_chunk_size => 10)
    EventMachine::run do
      EM.next_tick {
        local_store["a"] = {"foo" => "bar1"}
        EM.next_tick {
          local_store["b"] = {"foo" => "bar2"}
          EM.next_tick {
            local_store["c"] = {"foo" => "bar3"}
            EM.next_tick {
              local_store.path_stores[0].compact do
                EM::stop
              end
            }
          }
        }
      }
    end
    #load LocalStore anew
    local_store2=Monotable::LocalStore.new(:store_paths=>[temp_dir])

    local_store2.chunks.keys.should == ["","b","c"]
    chunk1 = local_store2.get_chunk("")
    chunk2 = local_store2.get_chunk("b")
    chunk3 = local_store2.get_chunk("c")
    chunk1.range_start.should == ""
    chunk1.range_end.should == chunk2.range_start
    chunk2.range_end.should == chunk3.range_start
    chunk3.range_end.should == Monotable::LAST_POSSIBLE_KEY
    chunk1.length.should == 1
    chunk2.length.should == 1
    chunk3.length.should == 1

    local_store2["a"].should == {"foo" => "bar1"}
    local_store2["b"].should == {"foo" => "bar2"}
    local_store2["c"].should == {"foo" => "bar3"}
  end

  it "2 chunks should automatically split in an evented fashion (sync)" do
    run_2_chunk_auto_split
  end

  it "2 chunks should automatically split in an evented fashion (async)" do
    Monotable::Journal.async_compaction=true
    run_2_chunk_auto_split
  end

end
