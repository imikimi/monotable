module Monotable
class Server
  class << self
    def default_port; 8080; end
    def default_host; "localhost"; end
    def default_host_and_port; "#{default_host}:#{default_port}"; end
  end

  # config variables
  attr_reader :port,:host,:options
  attr_reader :start_time
  attr_accessor :verbose

  # server module instances
  attr_reader :local_store,:router,:cluster_manager,:load_balancer,:global_index,:periodic_tasks

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

    @start_time = Time.now



    @local_store = Monotable::LocalStore.new(options)
    @router = Monotable::Router.new(self)
    @cluster_manager = Monotable::ClusterManager.new(self)
    @load_balancer = Monotable::LoadBalancer.new(self)
    @global_index = Monotable::GlobalIndex.new(self)
    @periodic_tasks = Monotable::PeriodicTasks.new(self)

    @port = options[:port] || Server.default_port
    @host = options[:host] || Server.default_host

    @cluster_manager.local_server = "#{@host}:#{@port}"

    initialize_new_store if options[:initialize_new_store]
  end

  def up_time
    Time.now - start_time
  end

  def initialize_new_store
    local_store.verify_store_is_blank_for_init
    max_chunk_size = local_store.max_chunk_size
    max_index_block_size = local_store.max_index_block_size

    num_index_levels = options[:num_index_levels] || 3
    raise ArgumentError.new("num_index_levels must be >=1") unless num_index_levels >= 1

    chunk_starts=[FIRST_DATA_KEY]
    num_index_levels.times do |level|
      chunk_starts << INDEX_KEY_PREFIX * (level+1) + FIRST_DATA_KEY
    end
    chunk_starts<<""
    chunk_starts.reverse!

    if options[:verbose]
      puts "Initializing new multi-store..."
      puts "  #{num_index_levels} index level(s)"
      puts "  #{max_chunk_size} bytes per chunk max"
      n = Math.log(max_chunk_size,2)
      m = num_index_levels
      address_bits = Tools.monotable_address_space_size(max_chunk_size,num_index_levels)
      puts "  Estimated max storage:        2^#{address_bits} bytes (#{Tools.commaize 2**(address_bits-40)} terabytes)"
      puts "  Estimated 'safe' max storage: 2^#{address_bits-2} bytes (#{Tools.commaize 2**(address_bits-42)} terabytes)"
      puts "  Initial chunk: #{chunk_starts.inspect}"
    end

    chunk_starts.each_with_index do |range_start,i|
      # TODO - this needs to also create the index records
      # This is probably not local_store's responsibility. since it requires access to the GlobalIndex.
      # Perhaps this should be done in Monotable::Server?
      chunk = local_store.add_chunk MemoryChunk.new(
        :max_chunk_size=>max_chunk_size,
        :max_index_block_size=>max_index_block_size,
        :range_start=>range_start,
        :range_end=>chunk_starts[i+1] || LAST_POSSIBLE_KEY
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
