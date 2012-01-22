module Monotable
module Daemon
module HTTP

class ServerController < RequestHandler

  def handle
    case "#{method}/#{action}"
    when "GET/chunks" then chunks
    when "GET/chunk" then chunk
    when "GET/servers" then servers
    when "GET/heartbeat" then heartbeat
    when "GET/local_store_status" then local_store_status
    when "PUT/join" then join # using PUT because its ok to join again if already joined
    when "POST/up_replicate_chunk" then up_replicate_chunk
    when "POST/down_replicate_chunk" then down_replicate_chunk
    when "POST/balance" then balance
    else handle_unknown_request
    end
  end

  # force load balancing
  def balance
    puts "balance: Monotable::Daemon::Server.cluster_manager.local_server = #{Monotable::Daemon::Server.cluster_manager.local_server}"
    puts "balance: neighbors: #{Monotable::Daemon::Server.cluster_manager.neighbors.inspect}"

    Monotable::Daemon::Server.load_balancer.balance
    respond 200,{:most_loaded_neighbor=>most_loaded_neighbor.to_s}
  end

  # server is joining the cluster
  def join
    server_name = params["server_name"]
    return handle_invalid_request("'server_name' parameter required") unless server_name
    Monotable::Daemon::Server.cluster_manager.add(server_name)
    servers # return our list of known servers
  end

  # get a list of known servers
  def servers
    servers={}
    Monotable::Daemon::Server.cluster_manager.servers.each do |k,v|
      servers[k] = v.to_hash
    end
    content={:servers => servers}
    respond 200, content
  end

  # get a list of chunks on this server
  def chunks
    content={:chunks=>Monotable::Daemon::Server.local_store.chunks.keys}
    respond 200, content
  end

  # get a list of chunks on this server
  def chunk
    return handle_invalid_request("chunk-id required") unless @resource_id
    chunk=Monotable::Daemon::Server.local_store.chunks[@resource_id]
    return handle_resource_missing_request("chunk-id:#{@resource_id.inspect}") unless chunk
    content={:records=>chunk.keys}
    respond 200, content
  end

  def heartbeat
    respond 200, {:status => :alive}
  end

  def up_replicate_chunk
    # compact chunk
    async_compaction=Journal.async_compaction
    Monotable::Daemon::Server.local_store.compact
    Journal.async_compaction=async_compaction

    # return chunk
    chunk=Monotable::Daemon::Server.local_store.chunks[@resource_id]
    respond_binary 200,chunk.chunk_file_data
  end

  def down_replicate_chunk
    Monotable::Daemon::Server.local_store.delete_chunk @resource_id
  end

  def local_store_status
    status = Monotable::Daemon::Server.local_store.status
    respond 200, status
  end
end

end
end
end
