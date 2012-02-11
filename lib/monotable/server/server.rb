module Monotable
class Server
  # config variables
  attr_accessor :port,:host,:verbose,:options

  # server module instances
  attr_accessor :local_store,:router,:cluster_manager,:load_balancer

  # options
  #   :store_paths=>["path",...]
  #   :port => Fixnum - TCP port to listen on, default 8080
  #   :host => host to listen on - default "localhost"
  #   :cluster => {} => params for initializing the cluster-manager
  def initialize(options={})
    @options = options
    @verbose=options[:verbose]
    if verbose
      puts "Monotable #{Monotable::VERSION}"
      puts "Startup options: #{options.inspect}"
      puts ""
    end

    @local_store = Monotable::LocalStore.new(options)
    @router = Monotable::Router.new :local_store=>@local_store
    @cluster_manager = Monotable::ClusterManager.new(options[:cluster])
    @load_balancer = Monotable::LoadBalancer.new(self)

    @port = options[:port] || 8080
    @host = options[:host] || 'localhost'

    @cluster_manager.local_daemon_address = "#{@host}:#{@port}"
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
