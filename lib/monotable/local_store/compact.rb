module MonoTable

  class DiskChunk

    def compact
      read_file=filename
      to_compact_file=filename+".to_compact"
      compacted_file=filename+".compacted"
      write_file=filename+".write"

      compactionlock.lock do
        if File.exists?(compacted_file) && MemoryChunk.verify(compacted_file)
          # valid chunk file already compacted
          return unless File.exists?(write_file) #nothing to do if no write
        end
        file
        if !File.exists?(to_compact_file)
          writelock.lock {File.move(write_file, to_compact_file)}
        end

        # read the last read-only version of the chunk
        chunk=MemoryChunk.new_from_file(:data=>read_file)

        # load and apply all edits in the
        chunk.load(compact_file)
        compaction file
        chunk.save(compacted_file)
        File.delete(to_compact_file)
        File.delete(read_file)
        File.move(compacted_file,read_file)
      end

    end

  end

end
