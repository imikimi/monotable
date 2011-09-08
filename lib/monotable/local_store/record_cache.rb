module Monotable
  class RecordCache
    class << self
      def init
        @cache=Cache.global_cache
        raise "no cache" unless @cache
      end

      def cache_key(record_key)
        [:record_cache,record_key]
      end

      def [](key) @cache.get(cache_key(key)) end
      def []=(key,value) @cache[cache_key(key)]=value end

      def get(key,&block) @cache.get(cache_key(key),&block) end
      def delete(key)     @cache.delete(key) end
    end
  end
  RecordCache.init
end
