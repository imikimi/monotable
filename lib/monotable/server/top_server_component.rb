module Monotable
class TopServerComponent
  attr_accessor :server

  def initialize(server)
    @server = server
  end

  def router; @router ||= server.router; end
  def local_store; @local_store ||= server.local_store; end
  def cluster_manager; @cluster_manager ||= server.cluster_manager; end
  def load_balancer; @load_balancer ||= server.load_balancer; end
  def global_index; @global_index ||= server.global_index; end
end
end
