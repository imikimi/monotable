# encoding: BINARY

# LocalStore is the entire local store managed by the daemon
# It manages 1 or more local paths
require "rbtree"

module Monotable
  module LocalStoreClientAPI
    #*************************************************************
    # Read API
    #*************************************************************
    # returns nil if the record does not exist
    def get(key,field_names=nil)
      record=RecordCache.get(key) do
        get_chunk(key).get_record(key)
      end
      record && record.fields(field_names)
    end

    #*************************************************************
    # Write API
    #*************************************************************
    def set(key,fields)     RecordCache[key]=get_chunk(key).set(key,fields) end
    def update(key,fields)  RecordCache[key]=get_chunk(key).update(key,fields) end
    def delete(key)         get_chunk(key).delete(key);RecordCache.delete(key) end
  end

  module LocalStoreChunkApi
    #*************************************************************
    # MemoryChunk API
    #*************************************************************
    # Throws errors if chunk for key not present
    def get_chunk(key) # rename chunk_for_record
      chunk_key,chunk=@chunks.upper_bound(key)
      raise "local chunks do not cover the key #{key.inspect}\nchunks: #{@chunks.keys.inspect}" unless chunk && chunk.in_range?(key)
      chunk
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
    include LocalStoreClientAPI
    include LocalStoreChunkApi

    #options
    #   :store_paths
    def initialize(options={})
      init_local_store(options)
    end

    def init_local_store(options={})
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
    end

    def initialize_new_multi_store
      @multi_store=true
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
        if @multi_store && chunk.range_start>""
          index_key,index_record=GlobalIndex.create_record_for_chunk(chunk)
          #puts "initialize_new_multi_store; adding chunk #{chunk.range_start.inspect}; index-record's key: #{[index_key,index_record].inspect}"
          # this should eventually call .set on the "router", not on self
          set(index_key,index_record)
        end
      else raise "unknown type #{chunk.class}"
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
