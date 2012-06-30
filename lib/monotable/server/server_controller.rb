module Monotable
module HttpServer

class ServerController < RequestHandler

  def record_key
    @record_key||=options[:record_key]
  end

  def local_store; server.local_store; end

  def handle
    case "#{method}/#{action}"
    when "GET/chunks" then chunks
    when "GET/chunk_status" then chunk_status
    when "GET/chunk_keys" then chunk_keys
    when "GET/servers" then servers
    when "GET/heartbeat" then heartbeat
    when "GET/local_store_status" then local_store_status
    when "GET/global_index_record" then global_index_record
    when "PUT/join" then join # using PUT because its ok to join again if already joined
    when "POST/update_servers" then update_servers

    when "GET/chunk" then get_chunk
    when "DELETE/chunk" then delete_chunk

    when "POST/clone_chunk" then clone_chunk
    when "POST/balance" then balance

    when "POST/chunk_replication" then set_chunk_replication
    when "POST/split_chunk" then split_chunk
    when "PUT/journal_entry" then journal_write

    when "PUT/up_replicate_chunk" then up_replicate_chunk
    when "PUT/down_replicate_chunk" then down_replicate_chunk
    when "PUT/move_chunk" then move_chunk
    else handle_unknown_request
    end
  end

  def heartbeat
    respond 200, {:result => :alive}
  end

  def chunk_request
    return handle_invalid_request("chunk-id required") unless @resource_id
    chunk = local_store.get_chunk(@resource_id)
    return respond 404, {:restult => "chunk not on server"} unless chunk
    respond 200, (yield chunk).merge(:result => :success)
  end

  # force load balancing
  def balance
    chunks_moved = server.load_balancer.balance
    respond 200,{:chunks_moved => chunks_moved}
  end

  private
  def cluster_manager
    @cluster_manager ||= server.cluster_manager
  end

  def add_servers(servers,skip_servers=[])
    num_known_servers = cluster_manager.servers.length
    cluster_manager.add_servers servers
    new_num_known_servers = cluster_manager.servers.length
    if num_known_servers != new_num_known_servers
      # if the server-list changed, inform the other servers of the update
      cluster_manager.broadcast_servers(skip_servers)
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
    chunk = local_store.get_chunk(@resource_id)
    return respond 404, {:restult => "chunk not on server"} unless chunk

    chunk.journal_write_and_apply(body,local_store)
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
    local_server_address = cluster_manager.local_server_address
    servers={local_server_address => {:server_address => local_server_address}}
    cluster_manager.servers.each do |k,v|
      servers[k] = v.to_hash
    end
    content={:servers => servers}
    respond 200, content
  end

  # get a list of chunks on this server
  def chunks
    content={:chunks=>local_store.chunks.keys}
    respond 200, content
  end

  # given any @resource_id as a key, selects the chunk that covers that key
  # returns info about the chunk on the server
  # returns 404 if chunk not found
  def chunk_status
    chunk_request do |chunk|
      {:status => chunk.status}
    end
  end

  # given any @resource_id as a key, selects the chunk that covers that key
  # returns a list of all record keys in a chunk
  # returns 404 if chunk not found
  def chunk_keys
    chunk_request do |chunk|
      {:keys => chunk.keys}
    end
  end

  def global_index_record
    record = server.global_index.find(@resource_id)
    respond 200, :result => :success, :key => record.key, :fields => record.fields
  end

  def set_chunk_replication    
    chunk_request do |chunk|
      chunk.replication_source = cluster_manager[params[:replication_source]]
      chunk.replication_client = cluster_manager[params[:replication_client]]
      {:status => chunk.status}
    end
  end

  def split_chunk
    on_key = @resource_id
    return handle_invalid_request("split-on key required") unless on_key
    chunk = local_store.get_chunk(on_key)
    return respond 404, {:restult => "chunk not on server"} unless chunk

    right_chunk = chunk.split(on_key)
    server.global_index.add_local_replica(right_chunk,true)
    respond 200, :result => "success", :chunks => [chunk.status, right_chunk.status]
  end

  def clone_chunk
    client_address = params[:from_server]

    client = cluster_manager[client_address]
    return respond 500, {:restult => "unknown from_server #{from_server.inspect}"} unless client

    chunk_data = client.chunk(@resource_id)
    chunk = local_store.add_chunk MemoryChunk.new(:data=>chunk_data)
    chunk.replication_source = client_address

    respond 200, {:result => :success}
  end

  def up_replicate_chunk
    chunk = local_store.get_chunk @resource_id
    return respond 404, {:restult => "chunk not on server"} unless chunk
    return respond 400, {:restult => "not master for chunk"} unless chunk.master?

    MasterChunk.new(server,chunk).up_replicate params[:to_server]
    respond 200, {:result => :success}
  end

  def down_replicate_chunk
    chunk = local_store.get_chunk @resource_id
    return respond 404, {:restult => "chunk not on server"} unless chunk
    return respond 400, {:restult => "not master for chunk"} unless chunk.master?
    
    MasterChunk.new(server,chunk).down_replicate params[:to_server]
    respond 200, {:result => :success}
  end

  def move_chunk
    chunk = local_store.get_chunk @resource_id
    return respond 404, {:restult => "chunk not on server"} unless chunk
    return respond 400, {:restult => "not master for chunk"} unless chunk.master?
    
    MasterChunk.new(server,chunk).move params[:from_server], params[:to_server]    
    respond 200, {:result => :success}
  end


  def get_chunk
    # compact chunk
    current_async_compaction = Journal.async_compaction
    Journal.async_compaction = false
    local_store.compact
    Journal.async_compaction = current_async_compaction

    chunk_request do |chunk|
      return respond_binary 200, chunk.chunk_file_data
    end
  end

  def delete_chunk
    chunk_request do |chunk|
      local_store.delete_chunk chunk.key
      {}
    end
  end

  def local_store_status
    respond 200, local_store.status
  end
end

end
end
