module Monotable
class ClusterManager
  attr_reader :servers, :local_server

  def initialize(options={})
    @servers = {}
  end

  def neighbors
    servers.select {|k,server| server!=local_server}
  end

  # sets the address of the local daemon
  def local_daemon_address=(server_address)
    @local_server=add(server_address)
  end

  def local_daemon_address; @local_server.to_s; end

  # add a server to the list of known servers
  def add(server_address)
    @servers[server_address] ||= ServerClient.new(server_address,:use_synchrony=>true)
  end

  def add_servers(servers)
    servers.each {|s| add(s)}
  end

  def join(server)
    client = add(server)
    join_result = client.join(local_server.to_s)
    add_servers join_result[:servers].keys
  end

  # return a simple, human and machine-readable ruby structure describing the status of the cluster
  def status
    {
    "local_daemon_address" => local_daemon_address,
    "known_servers" => servers.keys
    }
  end
end
end
