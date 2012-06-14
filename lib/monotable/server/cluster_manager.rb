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

  def [](server_name) servers[server_name] end

  # add a server to the list of known servers
  def add(server_address)
    @servers[server_address] ||= ServerClient.new(server_address,:internal=>true)
  end

  # server_client just returns the client for a given server or adds it if unknown - works the same as "add"
  alias :server_client :add

  def broadcast_servers(skip_servers=[])
    if @broadcast_servers_queued
      return
    end
    @broadcast_servers_queued=true

    skip_servers << local_server_address unless skip_servers.index(local_server_address)
    skip_server_param = skip_servers.clone
    servers.each do |name,client|
      skip_server_param << name unless skip_server_param.index(name)
    end

    EventMachine::Synchrony.add_timer(0) do
      @broadcast_servers_queued=false
      server_names = servers.keys
      servers.each do |name,client|
        next if skip_servers.index(name)
        client.update_servers(server_names,skip_server_param)
      end
    end
  end

  # servers is an array of server-addresses as strings
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
      return server if server.chunk_info ""
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
