# encoding: BINARY

# LocalStore is the entire local store managed by the daemon
# It manages 1 or more local paths
require "rbtree"

module MonoTable
  class LocalStore
    attr_accessor :chunks
    attr_accessor :path_stores

    def initialize(store_paths)
      @chunks=RBTree.new
      store_paths=[store_paths] unless store_paths.kind_of?(Array)
      @path_stores=store_paths.collect do |path|
        ps=PathStore.new(path,self)
        ps.chunks.each do |filename,chunk_file|
          chunks[chunk_file.range_start]=chunk_file
        end
        ps
      end
      initialize_new_store if chunks.length==0
    end

    def initialize_new_store
      chunk=Chunk.new
      chunk_file=@path_stores[0].add(chunk)
      chunks[chunk_file.range_start]=chunk_file
    end

    #*************************************************************
    # Read API
    #*************************************************************
    def get(key,field_names=nil)
      get_chunk(key).get(key,field_names)
    end

    #*************************************************************
    # Chunk API
    #*************************************************************
    # Throws errors if chunk for key not present
    def get_chunk(key) # rename chunk_for_record
      chunk_key,chunk=@chunks.upper_bound(key)
      raise "local chunks do not cover the key #{key.inspect}" unless chunk && chunk.in_range?(key)
      chunk
    end

    def chunk_keys
      @chunks.keys
    end

    def next_chunk(chunk)
      @chunks.lower_bound(chunk.range_end)[1]
    end

    #*************************************************************
    # Write API
    #*************************************************************
    def set(key,fields)     get_chunk(key).set(key,fields) end
    def update(key,fields)  get_chunk(key).update(key,fields) end
    def delete(key)         get_chunk(key).delete(key) end

    #*************************************************************
    # Internal API
    #*************************************************************
    def add(chunk)
      case chunk
      when ChunkFile then chunks[chunk.range_start]=chunk
      else raise "unknown type #{chunk.class}"
      end
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
