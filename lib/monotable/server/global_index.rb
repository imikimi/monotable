module Monotable
  class GlobalIndex < TopServerComponent

    class ChunkIndexRecord < MemoryRecord # TODO - why doesn't this work? MemoryRecord not defined... ?
      attr_reader :servers

      # initialize from an existing monotable_record
      # OR a chunk
      # OR key for new chunk
      def initialize(initializer,servers=nil)
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

      def fields(columns_hash=nil)
        {"servers"=>@servers.join(",")}
      end

      def add_server(server)
        server = server.to_s
        @servers << server
      end

      def remove_server(server)
        server = server.to_s
        puts "#{self.class}#remove_server(#{server.inspect})"
        @servers = @servers.select {|a| a!=server}
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
        @first_server=nil
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

    def find(internal_key,initializing=false)
      return first_record if (internal_key[/^\+*/]).length >= index_depth

      index_record_key=INDEX_KEY_PREFIX+internal_key # note, this doesn't have to be the exact key, just >= they key and < the next key
      #puts "#{self.class}#find index_record_key=#{index_record_key} index_depth=#{index_depth} first_record_key=#{first_record_key.inspect}"
      response = request_router.get_last(:lte=>index_record_key,:limit=>1)
      record = response[:records][0]
      raise MonotableDataStructureError.new("could not find index-record for chunk containing record: #{internal_key.inspect}. Index record-key: #{index_record_key.inspect}. Response=#{response.inspect}") unless record
      ChunkIndexRecord.new record
    rescue MonotableDataStructureError => ds_error
      return ChunkIndexRecord.new internal_key if initializing
      raise
    end

    # returns the server-list for servers that hold the chunk that contains the record for internal_key
    # NOTE: first server in the list is the server to write to; any can be read from
    def chunk_servers(internal_key)
      find(internal_key).servers
    end

    def update_chunk_server_list(chunk,initializing=false)
      ir = find(chunk.range_start,initializing)
      ir_old_fields = ir.fields.clone
      yield ir

      #puts "update_chunk_server_list. chunk.range_start = #{chunk.range_start[0..10].inspect}"
      #puts "update_chunk_server_list. ir.key = #{ir.key[0..10].inspect}"

      if chunk.range_start==""
        #puts "TODO - update the root/paxos record"
        # TODO - update the root/paxos record
      else
        puts "#{self.class}#update_chunk_server_list() key=#{ir.key.inspect} #{ir_old_fields.inspect} => #{ir.fields.inspect}"
        request_router.set ir.key, ir.fields
      end
    end

    def add_local_replica(chunk,initializing=false)
      puts "#{self.class}#add_local_replica(#{chunk.to_s.inspect}) pid=#{Process.pid} server=#{server.to_s.inspect}"
      update_chunk_server_list(chunk,initializing) {|ir| ir.add_server server}
    end

    def remove_local_replica(chunk)
      puts "#{self.class}#remove_local_replica(#{chunk.to_s.inspect}) pid=#{Process.pid} server=#{server.to_s.inspect}"
      update_chunk_server_list(chunk) {|ir| ir.remove_server server}
    end
  end
end
