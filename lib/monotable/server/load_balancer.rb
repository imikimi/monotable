module Monotable
class LoadBalancer
  attr_accessor :server

  def initialize
    @server=Monotable::Daemon::Server
  end

  def cluster_manager
    @cluster_manager||=server.cluster_manager
  end

  def local_store
    @local_store ||= server.local_store
  end

  def neighbor_chunks
    cluster_manager.neighbors.collect do |k,neighbor|
      [neighbor,neighbor.chunks]
    end
  end

  # returns [neighbor <ServerClient>, chunks <Array of chunk-keys>]
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

    # if the most_loaded_neighbor has 2 or more chunks than we do, move some over here!
    while chunks.length+1 > local_store.chunks.length
      chunk_key = chunks.pop
      chunk_data = client.up_replicate_chunk chunk_key
      local_store.add_chunk chunk_data

      client.down_replicate_chunk chunk_key
    end
  end
end
end
