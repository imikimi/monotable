module Monotable
class LoadBalancer < TopServerComponent

  def neighbor_chunks
    cluster_manager.neighbors.collect do |k,neighbor|
      [neighbor,neighbor.chunks]
    end
  end

  # Returns the "neighbor" that is most loaded and a list of its chunks:
  #   [neighbor <ServerClient>, chunks <Array of chunk-keys>]
  def most_loaded_neighbor
    neighbor_chunks.sort_by {|a| a[1].length}[-1]
  end

  # this is an overly simplistic implementation to get us to first base
  # It only measures load by the number of chunks each neighbor has
  # It then picks the most loaded neighbor, and if it is more loaded then us, it moves some
  # chunks from that neighbor to here.
  #
  # Eventually:
  #   Load should balanced along each of two dimensions:
  #     "Spacial balance": Free-bytes in store on each neighbor
  #     "Temperature balance": cpu/disk/net utilization of each neighbor
  #   Chunks to "move" should be selected based on:
  #     Their byte-size or
  #     Their "temperature" - a measure of the read/write activity related to that chunk
  #   Balance should be run periodically as a background task and therefor only needs to make some
  #     progres each run.
  #
  def balance
    client,chunks = self.most_loaded_neighbor
    #puts "#{self.class}#balance local chunks: #{local_store.chunks.keys.inspect}"
    #puts "#{self.class}#balance neighbor (#{client}) chunks: #{chunks.inspect}"

    chunks_moved={}
    # if the most_loaded_neighbor has 2 or more chunks than we do, move some over here!
    while chunks.length+1 > local_store.chunks.length
      chunk_key = chunks.pop
      #puts "moving chunk: #{chunk_key.inspect}"
      puts "#{self.class}#balance() moving chunk: #{chunk_key.inspect}"
      chunk_data = client.up_replicate_chunk chunk_key
      puts "#{self.class}#balance() moving chunk data = #{chunk_data.inspect}"
      chunk = local_store.add_chunk chunk_data
      #puts "update global index"
      global_index.add_local_replica(chunk)

      client.down_replicate_chunk chunk_key
      chunks_moved[chunk_key]=client.to_s
    end
    chunks_moved
  end

  private
  # takes a neighbor and its list of chunks, and moves one chunk at a time to us until balanced
  def async_balance_neighbor(neighbor,chunks,chunks_moved={},&block)

    # if the most_loaded_neighbor has at most 1 or more chunks than we do, we're balanced enough
    return yield chunks_moved if chunks.length+1 <= local_store.chunks.length

    chunk_key = chunks.pop
    neighbor.up_replicate_chunk chunk_key do
      local_store.add_chunk chunk_data
      client.down_replicate_chunk chunk_key do
        chunks_moved[chunk_key]=client.to_s
        async_balance_neighbor(neighbor,chunks,chunks_moved,&block) # async recursion
      end
    end
  end
  public

  def async_balance(&block)
    most_loaded_neighbor {|n,c| async_balance_neighbor(n,c,&block)}
  end

end
end
