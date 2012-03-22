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
      normalized_options = Tools.normalize_range_options(options)
      gte_key=normalized_options[:gte]
      get_chunk(gte_key).get_first(options)
    end

    # see ReadAPI
    def get_last(options={})
      normalized_options = Tools.normalize_range_options(options)
      lte_key=normalized_options[:lte]
      get_chunk(lte_key).get_last(options)
    end
  end

  # external facing Write API
  module LocalStoreWriteAPI
    include WriteAPI

    def set(key,fields)
      chunk = get_chunk(key)
      ret = chunk.set(key,fields)
      RecordCache[key] = chunk.get_record(key)
      ret
    end

    def update(key,fields)
      chunk = get_chunk(key)
      ret = chunk.update(key,fields)
      RecordCache[key] = chunk.get_record(key)
      ret
    end

    def delete(key)
      ret=get_chunk(key).delete(key)
      RecordCache.delete(key)
      ret
    end
  end

  module LocalStoreChunkApi
    # returns the chunk responsible for storing records at location "key"
    # returns nil for chunks not present
    def get_chunk(key) # rename chunk_for_record
      chunk_key,chunk=@chunks.upper_bound(key)
      chunk if chunk && (chunk.range_end==:infinity || key < chunk.range_end)
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

    def has_storage?
      path_stores.length > 0
    end

    def init_local_store(options={})
      puts "LocalStore initializing..." if options[:verbose]
      @options=options
      Monotable::Global.reset
      @max_chunk_size = options[:max_chunk_size] || DEFAULT_MAX_CHUNK_SIZE
      @max_index_block_size = options[:max_index_block_size] || DEFAULT_MAX_INDEX_BLOCK_SIZE

      @chunks=RBTree.new
      store_paths = options[:store_paths] || []
      @path_stores=store_paths.collect do |path|
        ps=PathStore.new(path,options.merge(:local_store=>self))
        ps.chunks.each do |filename,chunk_file|
          chunks[chunk_file.range_start]=chunk_file
        end
        ps
      end
      initialize_new_test_store if options[:initialize_new_test_store]
      if options[:verbose]
        puts "LocalStore successfully initialized."
        puts({"LocalStore status" => status}.to_yaml)
      end
    end

    def accounting_size
      @path_stores.inject(0) {|total,ps| ps.accounting_size+total}
    end

    def record_count
      @path_stores.inject(0) {|total,ps| ps.record_count+total}
    end

    # return a simple, human and machine-readable ruby structure describing the status of the cluster
    def status
      path_store_status = @path_stores.collect {|ps| ps.status}
      {
      :chunk_count => @chunks.length,
      :path_stores => @path_stores.collect {|ps| ps.status},
      :accounting_size => path_store_status.inject(0) {|total,ps| total+ps[:accounting_size]},
      :record_count => path_store_status.inject(0) {|total,ps| total+ps[:record_count]},
      }
    end

    def verify_store_is_blank_for_init
      path_stores_with_chunks = @path_stores.select {|path_store|path_store.chunks.length>0}
      if path_stores_with_chunks.length > 0
        path_list = path_stores_with_chunks.collect{|ps|"  "+ps.path.inspect}
        raise UserInterventionRequiredError.new [
          "Cannot initialize a new store. The following store locations already contain data:",
          "",
          path_list,
          "",
          "Options:",
          "  1) Use the existing store data. Start daemon without initializing a new store.",
          "  2) Delete the existing data",
          "  3) Use different store locations."
          ].flatten.join("\n")
      end
    end

    # a test-store is a 100% blank store
    # Significantly, it contains no index records
    def initialize_new_test_store
      verify_store_is_blank_for_init
      add_chunk MemoryChunk.new(:max_chunk_size=>max_chunk_size,:max_index_block_size=>max_index_block_size)
    end

    #*************************************************************
    # Internal API
    #*************************************************************
    def most_empty_path_store
      path_stores[0]  # temporary hack
    end

    def add_chunk(chunk)
      path_store = chunk.kind_of?(DiskChunk) ? chunk.path_store : most_empty_path_store
      disk_chunk = path_store.add_chunk chunk

      chunks[disk_chunk.range_start] = disk_chunk
    end

    def delete_chunk(chunk_id)
      chunk = chunks[chunk_id]
      raise "chunk does not exist: #{chunk_id.inspect}" unless chunk

      path_store = chunk.path_store

      path_store.delete_chunk(chunk)

      chunks.delete(chunk_id)
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
