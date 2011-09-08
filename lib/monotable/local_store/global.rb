module Monotable

  class Global
    class << self
      def reset
        Cache.global_cache.reset
        DiskChunk.reset_disk_chunks
      end
    end
  end
end
