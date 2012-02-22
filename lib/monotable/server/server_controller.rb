module Monotable
module HttpServer

class ServerController < RequestHandler

  def record_key
    @record_key||=options[:record_key]
  end

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
    when "POST/split_chunk" then split_chunk
    else handle_unknown_request
    end
  end

  # force load balancing
  def balance
    chunks_moved = server.load_balancer.balance
    respond 200,{:chunks_moved => chunks_moved}
  end

  # server is joining the cluster
  def join
    server_name = params["server_name"]
    return handle_invalid_request("'server_name' parameter required") unless server_name
    server.cluster_manager.add(server_name)
    servers # return our list of known servers
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

  # get info about a chunk on the server
  def chunk
    return handle_invalid_request("chunk-id required") unless @resource_id
    chunk = server.local_store.chunks[@resource_id]
    if chunk
      respond 200, :status => "found", :chunk_info => chunk_info(chunk)
    else
      respond 404, {:status => "chunk no on server"}
    end
  end

  def chunk_info(chunk)
    {
      :range_start => chunk.range_start,
      :range_end => chunk.symbolless_range_end,
      :accounting_size => chunk.accounting_size,
      :record_count => chunk.length
    }
  end

  def split_chunk
    return handle_invalid_request("chunk-id required") unless @resource_id
    chunk = server.local_store.chunks[@resource_id]
    if chunk
      on_key = params[:on_key]
      right_chunk = chunk.split(on_key)
      respond 200, :status => "success", :chunks => [chunk_info(chunk), chunk_info(right_chunk)]
    else
      respond 404, {:status => "chunk no on server"}
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
    puts "#{self.class}#up_replicate_chunk local_server = #{server.cluster_manager.local_server}"
    current_async_compaction = Journal.async_compaction
    server.local_store.compact
    Journal.async_compaction = current_async_compaction

    # return chunk
    chunk=server.local_store.chunks[@resource_id]
    chunk_data = chunk.chunk_file_data
    puts "#{self.class}#up_repilcate_chunk() chunk=#{chunk.file_handle}|#{chunk.to_s.inspect} chunk_data="+chunk_data.inspect
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
