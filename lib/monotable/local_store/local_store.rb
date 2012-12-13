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
      # find closest chunk
      chunk_key,chunk=@chunks.upper_bound(key)

      # return if it covers key
      chunk && chunk.cover?(key) && chunk
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
    attr_reader :path_stores
    attr_reader :store_paths

    #keep track of the chunk filenames so we can generate new, unique ones
    attr_accessor :chunks_by_basename

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
      @chunks_by_basename = {}
      @options = options
      Monotable::Global.reset
      @max_chunk_size = options[:max_chunk_size] || DEFAULT_MAX_CHUNK_SIZE
      @max_index_block_size = options[:max_index_block_size] || DEFAULT_MAX_INDEX_BLOCK_SIZE

      @chunks=RBTree.new
      @store_paths = options[:store_paths] || []
      @path_stores = @store_paths.collect do |path|
        ps = PathStore.new(path,options.merge(:local_store=>self))
        ps.chunks.each do |filename,chunk|
          #puts "#{self.class}#init_local_store add_chunk_internal filename=#{filename}"
          add_chunk_internal chunk
        end
        ps
      end
      @path_stores.each do |ps|
        ps.compact_existing_journals
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

    private
    def add_chunk_internal(chunk)
      raise InternalError.new "Non-unique chunk filename found! Chunks:\n"+
        "Chunk1: #{chunks_by_basename[chunk.basename].basename.inspect}\n"+
        "Chunk2: #{chunk.basename.inspect}\n" if chunks_by_basename[chunk.basename]
      raise "chunk does not have a path_store" unless chunk.path_store
      chunks_by_basename[chunk.basename] =
      chunks[chunk.key] = chunk
    end
    public

    #*************************************************************
    # Internal API
    #*************************************************************

    # generate a unique filename, ideally just an MD5 hash of the range_start, but if not, add a salt until it is unique
    def generate_basename(chunk)
      salt = nil
      while true
        filename = Digest::MD5.hexdigest("#{chunk.range_start}#{salt}") + CHUNK_EXT
        return filename unless chunks_by_basename[filename]
        salt = (salt||0) + 1
      end
    end

    def path_stores_with_free_space
      path_stores.collect {|ps| {path_store:ps, free_space:ps.free_space}}
    end

    def most_empty_path_store
      path_stores_with_free_space.max {|p1,p2| p1[:free_space] <=> p2[:free_space]}[:path_store]
    end

    def most_full_path_store
      path_stores_with_free_space.min {|p1,p2| p1[:free_space] <=> p2[:free_space]}[:path_store]
    end

    def add_chunk(chunk)
      chunk.basename ||= generate_basename chunk
      path_store = chunk.kind_of?(DiskChunk) ? chunk.path_store : most_empty_path_store
      disk_chunk = path_store.add_chunk chunk

      add_chunk_internal disk_chunk
    end

    def delete_chunk(chunk)
      chunk = chunks[chunk] if chunk.kind_of? String
      raise "chunk does not exist: #{chunk_id.inspect}" unless chunk

      chunk.path_store.delete_chunk(chunk)
      chunks.delete(chunk.key)
    end

    def update_path_store(chunk)
      if chunk.path_store
        return unless chunk.path_store_changed?
        chunk.path_store.remove_chunk(chunk)
      end
      path_stores.each do |ps|
        return ps.add_chunk(chunk) if ps.contains_chunk?(chunk)
      end
      raise InternalError.new "PathStore for chunk #{chunk.filename.inspect} could not be found. PathStores:\n PathStore: #{path_stores.collect{|ps|ps.path}.join(' PathStore\n')}"
    end

    def reset_chunk(full_chunk_path)
      #puts "reset_chunk full_chunk_path=#{full_chunk_path}"
      raise InternalError.new "chunk #{full_chunk_path.inspect} doesn't exist" unless File.exists?(full_chunk_path)
      basename = File.basename(full_chunk_path)

      # the only time a chunk won't exist in chunks_by_basename is if we are doing a
      # compaction as part of the local_store init.
      chunk = chunks_by_basename[basename]
      raise "chunk not set" unless chunk
      chunk.reset full_chunk_path
      update_path_store chunk
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
        raise "key=#{key.inspect} doesn't match chunk.key=#{chunk.key.inspect}" unless key==chunk.key
        raise "consecutive range keys out of order last_key=#{last_key.inspect} chunk.range_start=#{chunk.range_start.inspect} chunk.range_end=#{chunk.range_end.inspect}" unless
          last_key==nil || last_key<=chunk.range_start
      end
    end
  end
end
