require File.join(File.dirname(__FILE__),"mono_table_helper_methods")
require File.join(File.dirname(__FILE__),"common_api_tests")

describe Monotable::Journal do
  include MonotableHelperMethods

  def blank_store(options={})
    reset_temp_dir
    Monotable::LocalStore.new({:store_paths=>[temp_dir],:initialize_new_test_store=>true}.merge(options))
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

  it "chunks should automatically split in an evented fashion" do
    local_store=blank_store(:max_chunk_size => 10)
    Monotable::Journal.async_compaction=true
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
    local_store2.get_chunk("").length.should == 1
    local_store2.get_chunk("b").length.should == 1
    local_store2.get_chunk("c").length.should == 1
    local_store2["a"].should == {"foo" => "bar1"}
    local_store2["b"].should == {"foo" => "bar2"}
    local_store2["c"].should == {"foo" => "bar3"}
  end

  it "chunks should automatically split in an evented fashion" do
    local_store=blank_store(:max_journal_size => 10)
    Monotable::Journal.async_compaction=true
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
    local_store2.chunks.keys.should == [""]
    local_store2.get_chunk("").length.should == 3
    local_store2["a"].should == {"foo" => "bar1"}
    local_store2["b"].should == {"foo" => "bar2"}
    local_store2["c"].should == {"foo" => "bar3"}
  end

end
