# encoding: BINARY

# LocalStore is the entire local store managed by the daemon
# It manages 1 or more local paths
require "rbtree"

module Monotable
  # see ReadAPI
  module LocalStoreReadAPI
    include ReadAPI

    # see ReadAPI
    def get_record(key)
      RecordCache.get(key) do
        chunk=get_chunk(key)
        chunk && chunk.get_record(key)
      end
    end

    # see ReadAPI
    def get_first(options={})
      Tools.normalize_range_options(options)
      gte_key=options[:gte]
      get_chunk(gte_key).get_first(options)
    end

    # see ReadAPI
    def get_last(options={})
      Tools.normalize_range_options(options)
      lte_key=options[:lte]
      get_chunk(lte_key).get_last(options)
    end
  end

  # external facing Write API
  module LocalStoreWriteAPI
    include WriteAPI

    def set(key,fields)
      chunk=get_chunk(key)
      ret=chunk.set(key,fields)
      RecordCache[key]=chunk.get_record(key)
      ret
    end

    def update(key,fields)
      chunk=get_chunk(key)
      ret=chunk.update(key,fields)
      RecordCache[key]=chunk.get_record(key)
      ret
    end

    def delete(key)
      ret=get_chunk(key).delete(key)
      RecordCache.delete(key)
      ret
    end
  end

  module LocalStoreChunkApi
    # Throws errors if chunk for key not present
    def get_chunk(key) # rename chunk_for_record
      chunk_key,chunk=@chunks.upper_bound(key)
      raise "local chunks do not cover the key #{key.inspect}\nchunks: #{@chunks.keys.inspect}" unless chunk && chunk.in_range?(key)
      chunk
    end

    # Returns "true" if a local chunk covers the key-range for a given key.
    # There may or may-not be an actuall record for that key, but a "get(key)" will return an authoritative answer.
    def local?(key)
      get_chunk(key) && true
    end

    def chunk_keys
      @chunks.keys
    end

    def next_chunk(chunk)
      @chunks.lower_bound(chunk.range_end)[1]
    end
  end

  class LocalStore
    attr_accessor :chunks
    attr_accessor :max_index_block_size
    attr_accessor :max_chunk_size
    attr_accessor :path_stores
    include LocalStoreReadAPI
    include LocalStoreWriteAPI
    include LocalStoreChunkApi

    #options
    #   :store_paths
    def initialize(options={})
      init_local_store(options)
    end

    def init_local_store(options={})
      @options=options
      Monotable::Global.reset
      @max_chunk_size = options[:max_chunk_size] || DEFAULT_MAX_CHUNK_SIZE
      @max_index_block_size = options[:max_index_block_size] || DEFAULT_MAX_INDEX_BLOCK_SIZE

      @chunks=RBTree.new
      store_paths = options[:store_paths]
      @path_stores=store_paths.collect do |path|
        ps=PathStore.new(path,options.merge(:local_store=>self))
        ps.chunks.each do |filename,chunk_file|
          chunks[chunk_file.range_start]=chunk_file
        end
        ps
      end
      initialize_new_store if options[:initialize_new_store]
      initialize_new_multi_store if options[:initialize_new_multi_store]
    end

    def initialize_new_multi_store
      puts "Initializing new multi-store..." if @options[:verbose]
      @multi_store=self
      [
      "",
      INDEX_KEY_PREFIX*3+FIRST_DATA_KEY,  # for 64meg chunks approx 2^16 records max at this index level
      INDEX_KEY_PREFIX*2+FIRST_DATA_KEY,  # for 64meg chunks approx 2^32 records max at this index level
      INDEX_KEY_PREFIX*1+FIRST_DATA_KEY,  # for 64meg chunks approx 2^48 records max at this index level
      FIRST_DATA_KEY                      # for 64meg chunks approx 2^74 bytes max at this index level
      ].each do |range_start|
        chunk=MemoryChunk.new(:max_chunk_size=>max_chunk_size,:max_index_block_size=>max_index_block_size,:range_start=>range_start)
        chunk_file=@path_stores[0].add(chunk)
        add chunk_file
      end
    end

    def initialize_new_store
      chunk=MemoryChunk.new(:max_chunk_size=>max_chunk_size,:max_index_block_size=>max_index_block_size)
      chunk_file=@path_stores[0].add(chunk)
      add chunk_file
    end


    #*************************************************************
    # Internal API
    #*************************************************************
    def add(chunk)
      case chunk
      when DiskChunk then
        chunks[chunk.range_start]=chunk
        GlobalIndex.update_index(chunk,@multi_store) if @multi_store      else raise "unknown type #{chunk.class}"
      end
    end

    def length
      total=0
      chunks.each {|a,c| total+=c.length}
      total
    end

    # options: see Journal#compact
    def compact(options={})
      path_stores.each {|path_store| path_store.compact(options)}
    end

    def verify_chunk_ranges
      last_key=nil
      chunks.each do |key,chunk|
        raise "key=#{key.inspect} doesn't match chunk.range_start=#{chunk.range_start.inspect}" unless key==chunk.range_start
        raise "consecutive range keys out of order last_key=#{last_key.inspect} chunk.range_start=#{chunk.range_start.inspect} chunk.range_end=#{chunk.range_end.inspect}" unless
          last_key==nil || last_key<=chunk.range_start
      end
    end
  end
end
