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

    end
  end
end
