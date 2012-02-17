module Monotable
  class GlobalIndex < TopServerComponent

    class ChunkIndexRecord < MemoryRecord # TODO - why doesn't this work? MemoryRecord not defined... ?
      attr_reader :servers

      # initialize from an existing monotable_record
      # OR a chunk
      # OR key for new chunk
      def initialize(initializer,servers=nil)
        case initializer
        when Chunk then
          self.key = INDEX_KEY_PREFIX + initializer.range_start
        when String then
          self.key = INDEX_KEY_PREFIX + initializer
        when nil then
          raise InternalError.new("#{self.class} was initialized incorrectly")
        else
          init(initializer.key,initializer.fields)
          @servers = initializer.fields["servers"].split(",").map {|a| a.strip}
        end

        @servers ||= servers || []
      end

      def fields(columns_hash=nil)
        {"servers"=>@servers.join("\n")}
      end

      def add_server(server)
        @servers << server
      end

      def remove_server(server)
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

    def find(internal_key,initializing=false)
      index_record_key=INDEX_KEY_PREFIX+internal_key # note, this doesn't have to be the exact key, just >= they key and < the next key
      #puts "#{self.class}#find index_record_key=#{index_record_key}"
      response = request_router.get_last(:lte=>index_record_key,:limit=>1)
      record = response[:records][0]
      raise MonotableDataStructureError.new("could not find index-record for chunk containing record: #{internal_key.inspect}. Index record-key: #{index_record_key.inspect}. Response=#{response.inspect}") unless record || initializing
      ChunkIndexRecord.new record||internal_key
    end

    def update_replica_list(chunk,initializing=false)
      ir = find(chunk.range_start,initializing)
      yield ir

      #puts "update_replica_list. chunk.range_start = #{chunk.range_start[0..10].inspect}"
      #puts "update_replica_list. ir.key = #{ir.key[0..10].inspect}"

      if chunk.range_start==""
        puts "TODO - update the root/paxos record"
        # TODO - update the root/paxos record
      else
        request_router.set ir.key, ir
      end
    end

    def add_local_replica(chunk,initializing=false)
      update_replica_list(chunk,initializing) {|ir| ir.add_server server}
    end

    def remove_local_replica(chunk)
      update_replica_list(chunk) {|ir| ir.remove_server server}
    end
  end
end
