module Monotable
class ClusterManager
  class Server
    attr_reader :name

    def initialize(name)
      @name = name
    end

    def to_s; name; end

    # when called from a parent to_json, two params are passed in; ignored here
    def to_json(a=nil,b=nil)
      {:name => name}.to_json
    end
  end

  attr_reader :servers

  def initialize(options={})
    @servers = {}
  end

  attr_reader :local_daemon_address

  # sets the address of the local daemon
  def local_daemon_address=(server_address)
    @local_daemon_address=add(server_address)
  end

  # add a server to the list of known servers
  def add(server_address)
    @servers[server_address] = Server.new(server_address)
  end

  # return a simple, human and machine-readable ruby structure describing the status of the cluster
  def status
    {
    "local_daemon_address" => local_daemon_address.to_s,
    "known_servers" => servers.keys
    }
  end
end
end
