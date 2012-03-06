=begin
SBD 2012-03-06:
This is a little tricker than I thought. The key we use to fetch an index
record is not the key of the index record. This means our cache is nearly
useless with the simple implementation - it will only cache the index-record
read for one record key from the chunk covered.

there-for we need:

a) to store a local RBTree of cached index records and

b) we need to store them by the index-record keys, not keys of records in the covered chunk

c) we need to determine the range_end for the chunk covered and note it in the
  cached index-record. This will require a two-case strategy: fetch 2 records,
  not 1 when reading the index-record - read the index record and the next
  record

  c1) if two records are returned, then the second's key is the range_end value

  c2) if only one record is returned, then the "next get-range"'s start-range
      key will suffice. It will be <= the actual range_end. Note that it isn't
      necessarilly the actually the key of the next record; it is the
      range_start of the chunk that contains the next record. This is all OK
      because 1) our caching doesn't need to be exact, just close and 2) this
      just means our cached index-record covers a smaller range than the actual
      index-record covers.

=end
module Monotable
  class GlobalIndexCache
    class << self
      def init
        @cache=Cache.global_cache
      end

      def cache_key(internal_key)
        [:global_index_cache,internal_key]
      end

      def [](internal_key) @cache.get(cache_key(internal_key)) end
      def []=(internal_key,value) @cache[cache_key(internal_key)]=value end

      #def get(internal_key,&block) @cache.get(cache_key(internal_key),&block) end
      def get(internal_key,&block) yield end # caching disabled

      def delete(internal_key) @cache.delete(cache_key(interanl_key)) end
    end
  end
  GlobalIndexCache.init
end
