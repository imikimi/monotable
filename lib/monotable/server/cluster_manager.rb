module Monotable
class ClusterManager
  class Server
    attr_reader :name

    def initialize(name)
      @name = name
    end

    # when called from a parent to_json, two params are passed in; ignored here
    def to_json(a=nil,b=nil)
      {:name => name}.to_json
    end
  end

  attr_reader :servers

  def initialize(options={})
    @servers = {}
  end

  def add(server_name)
    @servers[server_name] = Server.new(server_name)
  end
end
end
