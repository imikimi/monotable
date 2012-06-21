module Monotable
class MasterChunk
  attr_accessor :chunk,:local_server

  def initialize(local_server,chunk)
    @chunk = chunk
    @local_server = local_server
    raise MonotableDataStructureError.new "this is not the Master server for chunk #{chunk.key.inspect}" unless chunk.master?
  end

  def cluster_manager; @cluster_manager||=local_server.cluster_manager; end

  def refresh_replication_chain(servers=global_index.chunk_servers(chunk))
    clients = [nil] + servers.collect {|s| cluster_manager[s]} + [nil]

    #TODO: <parallel>
    clients.each_with_index do |client,i|
      client && client.set_replication clients[i-1],clients[i+1]
    end
    #</parallel>
  end

  def down_replicate(to_server)
    to_client = cluster_manager[to_server]
    servers = global_index.remove_replica(chunk,to_client)
    refresh_replication_chain(servers)

    to_client.delete_chunk chunk
  end

  def up_replicate(to_server)
    to_client = cluster_manager[to_server]

    to_client.clone_chunk(chunk.key,cluster_manager.local_server_address)

    servers = global_index.add_replica(chunk,to_client)
    refresh_replication_chain(servers)
  end

  def move_chunk(from_server,to_server)
    up_replicate(to_server)
    down_replicate(from_server)
  end
end
end
