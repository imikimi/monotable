module Monotable
# this api is supported for internal use, it should never be used by an actual client
module ServerClientInternalAPI
  def chunks; request(:get,"server/chunks")[:chunks]; end
  def servers; request(:get,"server/servers")[:servers]; end
  def local_store_status; request(:get,"server/local_store_status"); end

  # returns nil if chunk not found
  def chunk_status(key); request(:get,"server/chunk_status/#{ue key}", :accept_404=>true)[:status]; end
  def chunk_keys(key); request(:get,"server/chunk_keys/#{ue key}", :accept_404=>true)[:keys]; end
  def chunk_replication(key); request(:get,"server/chunk_replication/#{ue key}", :accept_404=>true); end
  def global_index_record(key); request(:get,"server/global_index_record/#{ue key}"); end

  # returns true if the server is up and responding to the heartbeat
  def up?
    request(:get,"server/heartbeat")[:result]=="alive";
  rescue Errno::ECONNREFUSED => e
  end

  def split_chunk(on_key); request(:post,"server/split_chunk/#{ue on_key}")[:chunks]; end
  def balance; request(:post,"server/balance"); end
  def join(server,skip_servers=[]); request(:put,"server/join?server_name=#{ue server}&skip_servers=#{ue skip_servers.join(',')}")[:servers]; end
  def update_servers(servers,skip_servers=[]); request(:post,"server/update_servers",:params=>{:servers=>servers.join(','), :skip_servers=>skip_servers.join(',')})[:servers]; end

  def journal_write(chunk,journal_write_string);
    request(
      :put,"server/journal_entry/#{chunk}",
      :body => journal_write_string,
      :content_type => "application/octet-stream"
    );
  end

  # returns the raw chunk-file
  def chunk(chunk_key); request(:get,"server/chunk/#{ue chunk_key}",:raw_response => true); end

  # tell server to clone a chunk from_server to its own local_store
  def clone_chunk(chunk_key,from_server); request(:post,"server/clone_chunk/#{ue chunk_key}",:params => {:from_server => from_server}); end
  def delete_chunk(chunk_key); request(:delete,"server/chunk/#{ue chunk_key}"); end

  def set_chunk_replication(chunk_key,source="",client=""); request(:post,"server/chunk_replication/#{ue chunk_key}", :params => {:replication_source => source, :replication_client => client})[:status]; end

  # must be called on the Master server for the chunk specified by chunk_key
  def up_replicate_chunk(chunk_key,to_server); request(:put,"server/up_replicate_chunk/#{ue chunk_key}", :params => {:to_server => to_server.to_s})[:status]; end
  def down_replicate_chunk(chunk_key,to_server); request(:put,"server/down_replicate_chunk/#{ue chunk_key}", :params => {:to_server => to_server.to_s})[:status]; end
  def move_chunk(chunk_key,from_server,to_server); request(:put,"server/move_chunk/#{ue chunk_key}", :params => {:from_server => from_server.to_s, :to_server => to_server.to_s})[:status]; end
end
end
