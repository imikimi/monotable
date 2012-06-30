module Monotable
  class GlobalIndex < TopServerComponent

    class ChunkIndexRecord < MemoryRecord
      attr_reader :servers
      attr_reader :next_index_record_key

      # initialize from an existing monotable_record
      # OR a chunk
      # OR key for new chunk
      def initialize(initializer,servers=nil,next_index_record_key=nil)
        @next_index_record_key = next_index_record_key
        case initializer
        when Chunk, String then self.key = GlobalIndex.index_key(initializer)
        when nil then
          raise InternalError.new("#{self.class} was initialized with 'nil'")
        else
          init(initializer.key,initializer.fields)
          @servers = initializer.fields["servers"].split(",").map {|a| a.strip}
        end

        @servers ||= servers || []
      end

      def evict
        GlobalIndexCache.evict(self)
      end

      def fields(columns_hash=nil)
        {"servers"=>@servers.join(",")}
      end

      def add_server(server)
        @servers = (servers + [server.to_s]).uniq
      end

      def remove_server(server)
        @servers.delete server
      end

      def master
        servers[0]
      end
    end

    attr_reader :chunk_index_records
    def initialize(server)
      super
      @chunk_index_records={}
    end

    def request_router
      RequestRouter.new(router,:forward=>true)
    end

    class << self
      # get the index_key for a chunk
      def index_key(chunk)
        INDEX_KEY_PREFIX + case chunk
        when Chunk  then chunk.range_start
        when String then chunk
        end
      end

      # get the index_record for a chunk
      def index_record(chunk,store)
        ChunkIndexRecord.new store.get_record(index_key(chunk))
      end
    end

    # current server known to host the paxos record (the first record in the entire monotable)
    # TODO: this is a hack; replace
    def first_server
      @first_server ||= begin
        if local_store.get_chunk ""
          #puts "paxos server is local"
          local_store
        else
          cluster_manager.locate_first_chunk.tap do |first_server|
            raise MonotableDataStructureError.new "could not locate the first-chunk on any server: #{cluster_manager.servers.keys.inspect}" unless first_server
          end
        end
      end
    end

    def first_record
      server = first_server
      first_record = server.get_first(:gte=>"",:limit=>1)[:records][0]
      unless first_record
        #puts "first_server may have changed. Was: #{first_server}"
        @first_server = nil
        server = first_server
        first_record = server.get_first(:gte=>"",:limit=>1)[:records][0]
        raise MonotableDataStructureError.new "paxos server (#{first_server}) does not have a first-record" unless first_record
      end
      GlobalIndex::ChunkIndexRecord.new(first_record)
    end

    #NOTE: this hard-caches the first-record-key
    # Someday the first-record-key may change, but currently are initializing the
    # store to be big enough for practically anyone's need, so adding another
    # index level won't be needed for quite a while.
    def first_record_key
      @first_record_key ||= first_record.key
    end

    # returns the number of index levels in the global-index
    # This is also the number of "+"s in the first_record_key
    def index_depth
      @index_depth ||= first_record_key[/\+*/].length
    end

    # find the ChunkIndexRecord for the chunk that covers internal_key.
    # Logs all work done in work_log (an array), if provided.
    def find(internal_key,work_log=nil)
      return first_record if (internal_key[/^\+*/]).length >= index_depth
      index_record_key=INDEX_KEY_PREFIX+internal_key # note, this doesn't have to be the exact key, just >= they key and < the next key

      # remote request
      response = request_router.get_last(:lte=>index_record_key,:limit=>1)

      response[:work_log].each {|e| work_log<<e} if work_log
      record = response[:records][0]
      return ChunkIndexRecord.new(record) if record

      raise MonotableDataStructureError.new("could not find index-record for chunk containing record: #{internal_key.inspect}. Index record-key: #{index_record_key.inspect}. Response=#{response.inspect}")
    end

    # same as #find, only cached
    def cached_find(internal_key,work_log=nil)
      cached=true
      GlobalIndexCache.get(internal_key) do
        cached=false
        find(internal_key,work_log)
      end.tap do
        work_log << {:on_server => router.local_server.to_s, :action_type => :cache_fetch, :action_details => [:global_index_record_read,internal_key]} if work_log && cached
      end
    end

    def clear_index_cache_entry(internal_key)
      GlobalIndexCache.delete internal_key
    end

    # returns the server-list for servers that hold the chunk that contains the record for internal_key
    # NOTE: first server in the list is the server to write to; any can be read from
    def chunk_servers(internal_key,work_log=nil)
      find(internal_key,work_log).servers
    end

    def chunk_master(internal_key,work_log=nil)
      chunk_servers(internal_key).master
    end

    # same as #chunk_servers, only cached
    def cached_chunk_servers(internal_key,work_log=nil)
      cached_find(internal_key,work_log).servers
    end

    def update_chunk_server_list(chunk,initializing=false)
      ir = if initializing
        ChunkIndexRecord.new chunk.range_start
      else
        find(chunk.range_start)
      end
      raise MonotableDataStructureError.new("could not find index record for chunk #{chunk}. Closest <= was #{ir.key}") if ir.key!=INDEX_KEY_PREFIX+chunk.range_start
      yield ir

      if chunk.range_start==""
        #puts "TODO - update the root/paxos record"
        # TODO - update the root/paxos record
      else
        request_router.set ir.key, ir.fields
      end
      ir.servers
    end

    def add_replica(chunk,server,initializing=false)
      update_chunk_server_list(chunk,initializing) {|ir| ir.add_server server}
    end

    def remove_replica(chunk,server)
      update_chunk_server_list(chunk) {|ir| ir.remove_server server}
    end

    def add_local_replica(chunk,initializing=false)
      update_chunk_server_list(chunk,initializing) {|ir| ir.add_server server}
    end

    def remove_local_replica(chunk)
      update_chunk_server_list(chunk) {|ir| ir.remove_server server}
    end
  end
end
