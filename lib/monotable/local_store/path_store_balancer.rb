module Monotable
  class PathStoreBalancer
    # PathStoreWrapper
    #
    # Adds:
    #   sorting by free_space
    #   vitual free_space tracking (free_space after moves happen)
    #
    class PathStoreWrapper
      attr_accessor :path_store

      def initialize(ps) @path_store = ps; end

      # free_space stores the "virtual" free-space - what will be free after all the moves occur.
      def free_space; @free_space ||= path_store.free_space; end

      def sort_chunks_by_size
        chunks_by_size.sort_by! {|chunk| chunk.estimated_file_size}
      end

      def chunks_by_size
        return @chunks_by_size if @chunks_by_size
        @chunks_by_size = path_store.chunks.values
        sort_chunks_by_size
      end

      def pop
        chunks_by_size.pop.tap do |chunk|
          @free_space += chunk.estimated_file_size
        end
      end

      def push(chunk)
        @free_space -= chunk.estimated_file_size
        chunk.move path_store
        chunks_by_size << chunk
        sort_chunks_by_size
      end

      def <=>(second)
        free_space <=> second.free_space
      end
    end

    # local_store accessor
    attr_accessor :local_store
    attr_accessor :max_balanced_path_store_free_space_delta

    #********************************************************
    # init
    #********************************************************
    def initialize(local_store)
      self.local_store = local_store
      @max_balanced_path_store_free_space_delta = DEFAULT_MAX_CHUNK_SIZE * 5
    end

    #********************************************************
    # Path-Store Balancing
    # NOTE: this only journals the proposed chunk-moves to bring things into balance
    # TODO: detect if we already have moves scheduled, take them into account, and only if we still need balancing, do more moving.
    #********************************************************
    def path_stores
      @path_stores ||= local_store.path_stores.collect {|ps| PathStoreWrapper.new(ps)}
    end

    def emptiest_path_store
      path_stores.max
    end

    def fullest_path_store
      path_stores.min
    end

    def max_free_space_delta
      emptiest_path_store.free_space - fullest_path_store.free_space
    end

    def unbalanced?
      max_free_space_delta > max_balanced_path_store_free_space_delta
    end

    def balance
      while unbalanced?
        emptiest_path_store.push fullest_path_store.pop
      end
    end
  end
end
