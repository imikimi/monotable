module Monotable
module HttpServer

class ServerController < RequestHandler

  def record_key
    @record_key||=options[:record_key]
  end

  def handle
    case "#{method}/#{action}"
    when "GET/chunks" then chunks
    when "GET/chunk_info" then chunk_info
    when "GET/chunk_keys" then chunk_keys
    when "GET/servers" then servers
    when "GET/heartbeat" then heartbeat
    when "GET/local_store_status" then local_store_status
    when "PUT/join" then join # using PUT because its ok to join again if already joined
    when "POST/update_servers" then update_servers

    when "GET/chunk" then get_chunk
    when "POST/clone_chunk" then clone_chunk
    when "DELETE/chunk" then delete_chunk
    when "POST/chunk_replication_clients" then set_chunk_replication_clients

    when "POST/balance" then balance
    when "POST/split_chunk" then split_chunk
    when "PUT/journal_entry" then journal_write
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

  def journal_write
    chunk = server.local_store.get_chunk(@resource_id)
    return respond 404, {:status => "chunk not on server"} unless chunk

    chunk.journal_write(body)
    respond 200, {:result => :success}
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
  def chunk_info
    return handle_invalid_request("chunk-id required") unless @resource_id
    chunk = server.local_store.get_chunk(@resource_id)
    return respond 404, {:status => "chunk not on server"} unless chunk
    respond 200, :status => "found", :chunk_info => chunk.status
  end

  # given any @resource_id as a key, selects the chunk that covers that key
  # returns a list of all record keys in a chunk
  # returns 404 if chunk not found
  def chunk_keys
    return handle_invalid_request("chunk-id required") unless @resource_id
    chunk = server.local_store.get_chunk(@resource_id)
    return respond 404, {:status => "chunk not on server", :keys => []} unless chunk
    respond 200, :status => "found", :keys => chunk.keys
  end

  def split_chunk
    on_key = @resource_id
    return handle_invalid_request("split-on key required") unless on_key
    chunk = server.local_store.get_chunk(on_key)
    return respond 404, {:status => "chunk not on server"} unless chunk

    right_chunk = chunk.split(on_key)
    server.global_index.add_local_replica(right_chunk,true)
    respond 200, :status => "success", :chunks => [chunk.status, right_chunk.status]
  end

  def heartbeat
    respond 200, {:status => :alive}
  end

  def clone_chunk
    client_address = params[:from_server]

    client = server.cluster_manager[client_address]
    return respond 500, {:status => "unknown from_server #{from_server.inspect}"} unless client

    chunk_data = client.chunk(@resource_id)
    chunk = server.local_store.add_chunk MemoryChunk.new(:data=>chunk_data)

    respond 200, {:result => :success}
  end

  def get_chunk
    # compact chunk
    current_async_compaction = Journal.async_compaction
    Journal.async_compaction = false
    server.local_store.compact
    Journal.async_compaction = current_async_compaction

    # return chunk
    chunk = server.local_store.chunks[@resource_id]
    return respond 404, {:status => "chunk not on server"} unless chunk

    chunk_data = chunk.chunk_file_data
    respond_binary 200,chunk_data
  end

  def set_chunk_replication_clients
    chunk = server.local_store.chunks[@resource_id]
    return respond 404, {:status => "chunk not on server"} unless chunk

    clients = params[:clients].split(",").collect do |to_server|
      server.cluster_manager[to_server].tap do |server_client|
        return respond 500, {:status => "unknown to_server #{to_server.inspect}"} unless server_client
      end
    end
    chunk.replication_clients = clients
    respond 200, {:result => :success}
  end

  def delete_chunk
    chunk = server.local_store.chunks[@resource_id]
    return respond 404, {:status => "chunk not on server"} unless chunk

    server.local_store.delete_chunk @resource_id
    respond 200, {:result => :success}
  end

  def local_store_status
    status = server.local_store.status
    respond 200, status
  end
end

end
end
