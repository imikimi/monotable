module MonoTable
  class SoloDaemon < LocalStore

    #*************************************************************
    # Write API
    #*************************************************************
    def set(key,fields)     chunk_write(key) {|chunk| chunk.set(key,fields)} end
    def update(key,fields)  chunk_write(key) {|chunk| chunk.update(key,fields)} end
    def delete(key)         chunk_write(key) {|chunk| chunk.delete(key)} end

    #*************************************************************
    #*************************************************************
    def chunk_write(key)
      res=yield chunk=get_chunk(key)
      process_events
      return res
    end

    # the initial version of the SoloDaemon will immediately take action FullEvents
    # The next version will execute these events in their own threads
    def process_events
      until EventQueue.empty?
        case event=EventQueue.pop
        when ChunkFullEvent then
#          puts "split chunk..."
          time=Time.now
          event.chunk.split
#          puts "split chunk done in #{(1000*(Time.now-time)).to_i}ms."
        when JournalFullEvent then
#          puts "compact journal..."
          time=Time.now
          event.journal.compact
#          puts "compact journal done in #{(1000*(Time.now-time)).to_i}ms."
        end
      end
    end

    def verify_chunk_ranges
      last_key=nil
      chunks.each do |key,chunk|
        raise "key=#{key.inspect} doesn't match chunk.range_start=#{chunk.range_start.inspect}" unless key==chunk.range_start
        raise "consecutive range keys do not match last_key=#{last_key.inspect} chunk.range_start=#{chunk.range_start.inspect} chunk.range_end=#{chunk.range_end.inspect}" unless
          last_key==nil || last_key==chunk.range_start
      end
    end
  end
end
