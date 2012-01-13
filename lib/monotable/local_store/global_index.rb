module Monotable
  class GlobalIndex

    class Record < MemoryRecord # TODO - why doesn't this work? MemoryRecord not defined... ?
      attr_reader :servers

      # initialize from an existing monotable_record
      def initialize(monotable_record=nil)
        if monotable_record
          init(monotable_record.key,monotable_record)
          @servers = fields["servers"].split(",").map {|a| a.strip}
        else
          @servers=[]
        end
      end

      def init_new(chunk,servers=nil)
        servers||=["localhost"]
        @servers=servers
        init INDEX_KEY_PREFIX+chunk.range_start
        self["servers"]=servers.join(",")
        self
      end
    end

    class <<self

      def find(internal_key,router)
        GlobalIndex::Record.new(router.get_last(:gle=>"+"+internal_key,:limit=>1)[0][:record])
      end

      # create a properly formatted global-index record (as a hash) for a given chunk and set of servers it resides on
      # returns key,record
      def create_record_for_chunk(chunk,servers)
        key=INDEX_KEY_PREFIX+chunk.range_start
        record={:servers=>servers.join(",")}
        return key,record
      end

      def update_root_index(chunk,router)
        # TODO update the root index record
      end

      def update_index(chunk,router)
        return update_root_index(chunk,router) if chunk.range_start==""

        servers = ["whome?"] # TODO - where are we going to get the real server-list?
        r=GlobalIndex::Record.new.init_new(chunk,servers)
        router.set(r.key,r)
      end
    end
  end
end
