module Monotable
class Server

  # config variables
  attr_reader :port,:host,:options
  attr_accessor :verbose

  # server module instances
  attr_reader :local_store,:router,:cluster_manager,:load_balancer,:global_index

  # options
  #   :store_paths=>["path",...]
  #   :port => Fixnum - TCP port to listen on, default 8080
  #   :host => host to listen on - default "localhost"
  def initialize(options={})
    @options = options
    @verbose = options[:verbose]
    if verbose
      puts "Monotable #{Monotable::VERSION}"
      puts "Startup options: #{options.inspect}"
      puts ""
    end

    @local_store = Monotable::LocalStore.new(options)
    @router = Monotable::Router.new(self)
    @cluster_manager = Monotable::ClusterManager.new(self)
    @load_balancer = Monotable::LoadBalancer.new(self)
    @global_index = Monotable::GlobalIndex.new(self)

    @port = options[:port] || 8080
    @host = options[:host] || 'localhost'

    @cluster_manager.local_server = "#{@host}:#{@port}"

    initialize_new_store if options[:initialize_new_store]
  end

  def initialize_new_store
    local_store.verify_store_is_blank_for_init
    puts "Initializing new multi-store..." if @options[:verbose]
    max_chunk_size = local_store.max_chunk_size
    max_index_block_size = local_store.max_index_block_size
    chunk_starts=[
      "",
      INDEX_KEY_PREFIX*3+FIRST_DATA_KEY,  # for 64meg chunks approx 2^16 records max at this index level
      INDEX_KEY_PREFIX*2+FIRST_DATA_KEY,  # for 64meg chunks approx 2^32 records max at this index level
      INDEX_KEY_PREFIX*1+FIRST_DATA_KEY,  # for 64meg chunks approx 2^48 records max at this index level
      FIRST_DATA_KEY                      # for 64meg chunks approx 2^74 bytes max at this index level
    ]
    chunk_starts.each_with_index do |range_start,i|
      # TODO - this needs to also create the index records
      # This is probably not local_store's responsibility. since it requires access to the GlobalIndex.
      # Perhaps this should be done in Monotable::Server?
      chunk = local_store.add_chunk MemoryChunk.new(
        :max_chunk_size=>max_chunk_size,
        :max_index_block_size=>max_index_block_size,
        :range_start=>range_start,
        :range_end=>chunk_starts[i+1] || :infinity
      )
      global_index.add_local_replica chunk, true
    end
  end

  # call this after monotable is listening to incoming HTTP request
  def post_init
    @cluster_manager.join(@options[:join]) if @options[:join]
  end

  def inspect
    "#<#{self.class} port=#{port} host=#{host}>"
  end

  def to_s
    "#{host}:#{port}"
  end
end
end
