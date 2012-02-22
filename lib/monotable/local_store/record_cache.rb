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

      def [](key) get(key) end
      def []=(key,value) @cache[cache_key(key)]=value end

      def get(key,&block)
        rec = @cache.get(cache_key(key),&block)

        # reset if the record is not valid
        # TODO: should we periodically go through the RecordCache and look for invalid records and prune them?
        if rec && !rec.valid?
          return self[key] = yield
        end
        rec
      end
      def delete(key)     @cache.delete(cache_key(key)) end
    end
  end
  RecordCache.init
end
