=begin
All values in the cache must respond to:

  # returns number of bytes of memory used by this object
  def memory_size();

Values in cache should not change their keys nor their memory_size.
=end

module MonoTable
  MAX_CACHE_SIZE=64*(1024**2)

  class Cache
    attr_reader :max_size,:size
    attr_reader :eviction_count,:eviction_bytes
    attr_reader :head,:tail

    def Cache.global_cache(max_cache_size=nil)
      @global_cache||=Cache.new(max_cache_size)
    end

    class ListNode
      attr_accessor :nnode, :pnode
      attr_reader :key,:value
      def initialize(key,value) @key=key;@value=value; end

      # remove the linked list
      # note, does not clear its own nnode and pnode - assuming they will be overwritten or discarded becore accessed again
      def remove
        pnode.nnode=nnode
        nnode.pnode=pnode
        self
      end

      # add node AFTER this node
      def add(node)
        node.nnode=nnode
        node.pnode=self
        nnode.pnode=node
        self.nnode=node
      end

      def to_s; key.inspect; end
      def inspect; {:key=>key,:nnode=>nnode.key.inspect,:pnode=>pnode.key.inspect}.inspect; end
    end

    def initialize(max_size=nil)
      @max_size=max_size || MAX_CACHE_SIZE

      reset
    end

    def reset
      @size=0
      @eviction_bytes=@eviction_count=0
      @head=ListNode.new(nil,nil)
      @tail=ListNode.new(nil,nil)

      @head.nnode=@tail
      @tail.pnode=@head

      @hash_cache={}
    end

    # remove the lowest-priority item from the cache
    def evict
      node=tail.pnode
      @eviction_bytes+=node.value.memory_size
      @eviction_count+=1
      delete tail.pnode.key
    end

    def length; @hash_cache.length; end

    def free(bytes=0)
      while @size + bytes > @max_size
        evict
      end
    end

    def [](key)
      get(key)
    end

    def get(key,&block)
      node=@hash_cache[key]
      unless node
        return self[key]=yield if block
        return nil
      end
      head.add(node.remove) # on use, move to the head of the list
      node.value
    end

    def []=(key,value)
      delete(key)
      ms=value.memory_size
      free ms
      @size+=ms
      node=@hash_cache[key]=ListNode.new(key,value)
      head.add node # add to the head of the list
      value
    end

    def each
      node=head.nnode
      while node!=tail
        yield node.key,node.value
        node=node.nnode
      end
    end

    def each_node
      node=head.nnode
      while node!=tail
        yield node
        node=node.nnode
      end
    end

    def keys
      ks=[]
      each {|k,v| ks<<k}
      ks
    end

    # returns nil if there is no node at key, else returns the removed node
    def delete(key)
      node=@hash_cache.delete(key)
      if node
        node.remove
        value=node.value
        @size-=value.memory_size
        value
      end
    end
  end


end
