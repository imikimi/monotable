module Monotable
  class GlobalIndex
    class <<self

      # create a properly formatted global-index record (as a hash) for a given chunk and set of servers it resides on
      # returns key,record
      def create_record_for_chunk(chunk,servers=nil)
        key=INDEX_KEY_PREFIX+chunk.range_start
        servers||=["localhost"]
        record={:servers=>servers.join(",")}
        return key,record
      end

      def update_root_index(chunk,router)
        # TODO update the root index record
      end

      def update_index(chunk,router)
        return update_root_index(chunk,router) if chunk.range_start==""

        index_key,index_record=create_record_for_chunk(chunk)
        router.set(index_key,index_record)
      end
    end
  end
end
