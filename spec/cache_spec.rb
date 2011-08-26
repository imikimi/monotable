require File.join(File.dirname(__FILE__),"../lib/monotable/monotable")
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe Monotable::Cache do
  include MonotableHelperMethods

  def validate_cache_list(cache,keys_compare,debug=false)
    keys_visited={}
    cache.each_node do |node|
      k=node.key
      puts "validate_cache_list node: #{node.inspect}" if debug
      if keys_visited[k]
        if debug
          raise "already visited key #{k.inspect}"
        else
          return validate_cache_list(cache,keys_compare,true)
        end
      end
      keys_visited[k]=true
    end
    cache.keys.should == keys_compare
  end

  def generate_cache(n,max_size=nil,val="abc")
    cache=Monotable::Cache.new(max_size)
    n.times do |key|
      key="key#{key+1}".to_sym
      cache[key]=TestCacheObject.new val
    end
    cache
  end

  class TestCacheObject
    def initialize(value)
      @value=value
    end

    attr_reader :value

    def memory_size
      @value.length
    end
  end

  it "should be possible to init a Cache" do
    cache=Monotable::Cache.new(1000)
    cache.max_size.should == 1000
  end

  it "not in cache should return nil" do
    cache=generate_cache(1)
    cache[:not_there].should == nil
  end

  it "should be possible store an object in the cache" do
    cache=generate_cache(1)
    cache[:key1].value.should == "abc"
    validate_cache_list cache, [:key1]
  end

  it "delete should work" do
    cache=generate_cache(1)
    cache.delete(:key1).value.should == "abc"
    cache[:key].should == nil
  end

  it "new entries should be at the head of the lru list" do
    cache=generate_cache(3)
    validate_cache_list cache, [:key3,:key2,:key1]
  end

  it "accessing a key should move it to the front of the lru list" do
    cache=generate_cache(3)
    cache[:key2]
    validate_cache_list cache, [:key2,:key3,:key1]
  end

  it "resetting a key should move it to the front of the lru list" do
    cache=generate_cache(3)
    cache[:key1]=TestCacheObject.new "def"
    validate_cache_list cache, [:key1,:key3,:key2]
  end

  it "eviction should remove the last element" do
    cache=generate_cache(3)
    validate_cache_list cache, [:key3,:key2,:key1]
    cache.evict
    validate_cache_list cache, [:key3,:key2]
  end

  it "cache should not exceed the specified max size" do
    cache=generate_cache(10,1000,"1"*100)
    cache.eviction_count.should==0
    cache.eviction_bytes.should==0
    cache.size.should == 1000
    cache[:next]=TestCacheObject.new "1"*100
    cache.size.should == 1000
    cache.eviction_count.should==1
    cache.eviction_bytes.should==100
  end
end
