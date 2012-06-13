module Monotable
Tools.debug
module HttpServer

class ServerController < RequestHandler

  def record_key
    @record_key||=options[:record_key]
  end

  def handle
    Tools.debug "#{method}/#{action}"
    case "#{method}/#{action}"
    when "GET/chunks" then chunks
    when "GET/chunk" then chunk
    when "GET/chunk_keys" then chunk_keys
    when "GET/servers" then servers
    when "GET/heartbeat" then heartbeat
    when "GET/local_store_status" then local_store_status
    when "PUT/join" then join # using PUT because its ok to join again if already joined
    when "POST/update_servers" then update_servers
    when "POST/up_replicate_chunk" then up_replicate_chunk
    when "POST/down_replicate_chunk" then down_replicate_chunk
    when "POST/balance" then balance
    when "POST/split_chunk" then split_chunk
    else handle_unknown_request
    end
  end

  # force load balancing
  def balance
    chunks_moved = server.load_balancer.balance
    respond 200,{:chunks_moved => chunks_moved}
  end

  private
  def add_servers(servers,skip_servers=[])
    num_known_servers = server.cluster_manager.servers.length
    server.cluster_manager.add_servers servers
    new_num_known_servers = server.cluster_manager.servers.length
    if num_known_servers != new_num_known_servers
      # if the server-list changed, inform the other servers of the update
      server.cluster_manager.broadcast_servers(skip_servers)
    end
  end
  public

  # server is joining the cluster
  def join
    server_name = params["server_name"]
    skip_servers = (params["skip_servers"]||"").split(",")
    return handle_invalid_request("'server_name' parameter required") unless server_name
    add_servers([server_name],skip_servers)
    self.servers # return our list of known servers
  end

  def update_servers
    servers = params["servers"].split(",")
    skip_servers = (params["skip_servers"]||"").split(",")
    add_servers(servers,skip_servers)
    self.servers # return our list of known servers
  end

  # get a list of known servers
  def servers
    local_server_address = server.cluster_manager.local_server_address
    servers={local_server_address => {:server_address => local_server_address}}
    server.cluster_manager.servers.each do |k,v|
      servers[k] = v.to_hash
    end
    content={:servers => servers}
    respond 200, content
  end

  # get a list of chunks on this server
  def chunks
    content={:chunks=>server.local_store.chunks.keys}
    respond 200, content
  end

  # given any @resource_id as a key, selects the chunk that covers that key
  # returns info about the chunk on the server
  # returns 404 if chunk not found
  def chunk
    return handle_invalid_request("chunk-id required") unless @resource_id
    chunk = server.local_store.get_chunk(@resource_id)
    if chunk
      respond 200, :status => "found", :chunk_info => chunk.status
    else
      respond 404, {:status => "chunk not on server"}
    end
  end

  # given any @resource_id as a key, selects the chunk that covers that key
  # returns a list of all record keys in a chunk
  # returns 404 if chunk not found
  def chunk_keys
    return handle_invalid_request("chunk-id required") unless @resource_id
    chunk = server.local_store.get_chunk(@resource_id)
    if chunk
      respond 200, :status => "found", :keys => chunk.keys
    else
      respond 404, {:status => "chunk not on server", :keys=>[]}
    end
  end

  def split_chunk
    on_key = @resource_id
    return handle_invalid_request("split-on key required") unless on_key
    chunk = server.local_store.get_chunk(on_key)
    if chunk
      right_chunk = chunk.split(on_key)
      server.global_index.add_local_replica(right_chunk,true)
      respond 200, :status => "success", :chunks => [chunk.status, right_chunk.status]
    else
      respond 404, {:status => "chunk not on server"}
    end
  end

  def heartbeat
    respond 200, {:status => :alive}
  end

  # returns the up-to-date chunk as a binary chunk-file
  # The final version should look more like this:
  #   1) caller starts streaming all NEW updates to the chunk
  #   2) all previous updates are compacted asynchronously
  #   3) caller receives the compacted chunk-file data
  #   4) caller adds itself to the chunk global-index-record's server-list
  #   1 & 2 would be started by this call, then 3 & 4 would be a processed in a callback
  def up_replicate_chunk
    # compact chunk
    current_async_compaction = Journal.async_compaction
    Journal.async_compaction = false
    server.local_store.compact
    Journal.async_compaction = current_async_compaction

    # return chunk
    chunk=server.local_store.chunks[@resource_id]
    chunk_data = chunk.chunk_file_data
    respond_binary 200,chunk_data
  end

  def down_replicate_chunk
    chunk=server.local_store.chunks[@resource_id]
    server.local_store.delete_chunk @resource_id
    server.global_index.remove_local_replica(chunk)
    respond 200, {:result => :success}
  end

  def local_store_status
    status = server.local_store.status
    respond 200, status
  end
end

end
end
