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

    chunks_moved={}
    # if the most_loaded_neighbor has 2 or more chunks than we do, move some over here!
    while chunks.length > local_store.chunks.length+1
      chunk_key = chunks.pop

      master_client = cluster_manager[global_index.chunk_master(chunk_key)]
      master_client.move_chunk(chunk_key,client,cluster_manager.local_server)
      chunks_moved[chunk_key]=client.to_s
    end
    chunks_moved
  end

end
end
