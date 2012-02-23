module Monotable
class ClusterManager < TopServerComponent
  attr_reader :servers
  attr_reader :local_server_address, :local_server

  def initialize(server)
    super
    @servers = {}
  end

  def neighbors
    remote_servers
  end

  def remote_servers
    servers.select {|k,server| server!=local_server}
  end

  def local_server=(local_server_address)
    @local_server_address = local_server_address
    @local_server = add(@local_server_address)
  end

  # add a server to the list of known servers
  def add(server_address)
    @servers[server_address] ||= ServerClient.new(server_address,:internal=>true)
  end

  # server_client just returns the client for a given server or adds it if unknown - works the same as "add"
  alias :server_client :add

  def add_servers(servers)
    servers.each {|s| add(s)}
  end

  def join(server)
    join_result = if @server.local_store.has_storage?
      server_client(server).join(@local_server_address)
    else
      res=server_client(server).servers
      res
    end
    add_servers join_result.keys
  end

  # this is an inefficient way to do this.
  # TODO: eventually this should use the PAXOS (or equiv) system
  def locate_first_chunk
    remote_servers.each do |k,server|
      return server if server.chunk ""
    end
    nil
  end

  # return a simple, human and machine-readable ruby structure describing the status of the cluster
  def status
    {
    "local_server_address" => local_server_address,
    "known_servers" => servers.keys
    }
  end
end
end
