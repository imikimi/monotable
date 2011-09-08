module Monotable
  class IndexBlockCache
    class << self
      def init
        @cache=Cache.global_cache
        raise "no cache" unless @cache
      end

      def cache_key(chunk,record_key)
        [:index_block_cache,chunk.memory_revision,record_key]
      end

      def [](chunk,key) @cache.get(cache_key(chunk,key)) end
      def []=(chunk,key,value) @cache[cache_key(chunk,key)]=value end

      def get(chunk,key,&block) @cache.get(cache_key(chunk,key),&block) end
    end
  end
  IndexBlockCache.init
end
