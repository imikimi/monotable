module Monotable
  class Compactor

    attr_accessor :chunks,:journal_file,:journal_filename

    def initialize(journal_filename)
      @journal_filename=journal_filename.to_s
      @chunks={}
    end

    def apply_entry(journal_entry,chunk)
      case journal_entry[:command]
      when :set then chunk.set(journal_entry[:key],journal_entry[:fields])
      when :delete then chunk.delete(journal_entry[:key])
      when :delete_chunk then File.delete journal_entry[:chunk_file]
      when :move_chunk then
        chunk.
        local_store.get_path_store(journal_entry[:path_store_path])
      when :split then
        chunk2=chunk.split(journal_entry[:key])
        chunks[journal_entry[:to_file]] = chunk2
      else
        raise InternalError.new "apply_entry: invalid journal_entry[:command]: #{journal_entry[:command].inspect}"
      end
    end

    def each_entry
      journal_file.read(0,nil,true) do |io_stream|
        while !io_stream.eof?
          yield Journal.parse_entry(io_stream)
        end
      end
    end

    def compaction_working_path
      @compaction_working_path ||= journal_filename + COMPACT_DIR_EXT
    end

    def successful_compaction_filename
      @successful_compaction_filename ||= File.join compaction_working_path, JOURNAL_COMPACTION_SUCCESS_FILENAME
    end

    # load all entries from a journal and cluster them by chunk_filename
    # Returns hash with exactly one entry per chunk_filename touched (including merged from or split-to)
    # NOTE: all chunks involved in merges and splits will share the same list of entries,
    #   though each will have its own entry in the returned structure.
    def load_entries
      entries_by_chunk={}
      each_entry do |entry|
        chunk_filename = entry[:chunk_file]

        if jes = entries_by_chunk[chunk_filename]
          jes<<entry
        else
          entries_by_chunk[chunk_filename]=[entry]
        end

        # All chunks involved in some combination of splits and merges will have all their journal entries
        # merged into one list to be processed in concert
        if entry[:command]==:split
          chunk2_filename = entry[:to_file]
          entries_by_chunk[chunk2_filename] = entries_by_chunk[chunk_filename]
        end
      end
      journal_file.close
      entries_by_chunk
    end

    # Given a list of entries, applies all edits to every chunk touched to complete MemoryChunk representations
    # Returns a hash of all chunk-files processed as: {chunk_file_name => MemoryChunk instance}
    #
    # NOTE: merging and splitting magic
    #   If a chunk is merged or split, then entries will contain entries for more than one chunk.
    #   Specifically, entries will contain all entries for all chunks involved in any combination of merges and splits.
    #   The order of the entries may not be the original order written to disk, but it will be a consitent ordering such that they can be processed in order safely.
    def apply_entries_in_memory(entries)
      entries.each do |entry|
        chunk_filename = entry[:chunk_file]
        chunks[chunk_filename] ||= MemoryChunk.load(chunk_filename)
        chunk = chunks[chunk_filename]
        apply_entry(entry,chunk)
      end
    end

    # given a hash {chunk_filename => entries},
    # Runs apply_entries_in_memory on each entries
    def compact_by_chunk(entries_by_chunk)
      completed_chunks={}
      entries_by_chunk.each do |chunk_filename,entries|
        # chunks involved in merges and splits will be processed together when the first representative of that
        # cluster is processed. Hence, when we attempt to process the other members later, we just skip them.
        next if completed_chunks[chunk_filename]

        apply_entries_in_memory entries
        chunks.each {|k,v| completed_chunks[k]=true}
        write_compacted_chunks
      end
    end

    # write all compacted chunks to disk
    # TODO: Write comacted_chunks to most-empty/least-loaded PathStores to Balance them
    def write_compacted_chunks
      chunks.each do |chunk_filename,chunk|
        new_chunk_filename=File.join(compaction_working_path, File.basename(chunk_filename))
        chunk.save new_chunk_filename
      end
    end

    def journal_file
      @journal_file ||= FileHandle.new(journal_filename)
    end

    # file is a FileHandle or filename
    # phase 1 does 99% of the work:
    #   * reads the journal
    #   * reads all the affected chunks
    #   * generates and writes the new versions of the chunks to a temporary directory.
    # Phase 1 can safely be run without blocking any server requests as long as one condition holds:
    #   The journal_file being compacted can no longer be written to.
    # In Phase 1, it is safe to:
    #   Read from the affected chunks (which includes reading from the journal file)
    #   Write to the affected chunks (as long as they are writing to a new journal)
    def compact_phase_1

      # test to see if this phase is already done
      return if compact_phase_1_succeeded

      self.chunks={}
      base_path=File.dirname(journal_filename)
      FileUtils.mkdir compaction_working_path unless File.exists?(compaction_working_path)

      if journal_file.exists?
        # apply journal to every chunk
        # TODO: if the journal is somehow corrupt, I think the right answer is to stop parsting the journal, but keep all changes so far and continue.
        # The chunks -may- be OK if there were no further writes to them in the rest of the corrupt journal. We can detect their valididity later or immeidately with replicas on other disks.
        # Perhaps we need a possibly-corrupt directory which implies we need to compare with the replicas to verify integrity... ?

        entries_by_chunk = load_entries

        compact_by_chunk entries_by_chunk

        # "touch" the file: JOURNAL_COMPACTION_SUCCESS_FILENAME
        File.open(successful_compaction_filename,"w") {}
      end
    end

    # executes the compaction phase-1 as an external processes
    # we are not using fork because it is important that we start with a blank garbage-collection space
    def compact_phase_1_external
      # test to see if there is actually any phase-1 work
      return if compact_phase_1_succeeded

      pid=nil
      ret=nil
      binary=File.join File.dirname(__FILE__), "../../../bin", "compact.rb"
      command="#{binary} #{journal_filename}"
      begin
        IO.popen(command) do |pipe|
          pid=pipe.pid
          ret=pipe.read
        end
      ensure
        Process.detach pid if pid
      end
      raise "compact_phase_1_external failed (ret=#{ret.inspect})" unless ret.strip=="SUCCESS"
      true
    end

    def compact_phase_1_succeeded
      (!journal_file.exists? && !File.exists?(compaction_working_path)) || File.exists?(successful_compaction_filename)
    end

    # file can be a filename, or any object that to_s outputs a filename (e.g. FileHandle)
    # Phase 2 is the cleanup phase:
    #   * deletes old versions of chunks and moves the new versions in place
    #   * deletes the journal file
    #   * removes compacted_chunks_path
    # Exclusive locks on each chunk must be aquired as its old version is deleted and new version is moved in place
    # TODO: Chunks effected should be "reset" after compaction
    def compact_phase_2
      # check to see if there is anything to do
      return unless journal_file.exists? || File.exists?(compaction_working_path)

      # verify the successfile exists
      raise "compact_phase_1 did not complete" unless File.exists?(successful_compaction_filename)

      # TODO: lock all the effected chunks until the end of this block
      #   Better-yet, as we save each chunk, we should update its in-memory DiskChunk.
      #   This will point to the file in the compacted_chunk_path, though.
      #   And then after we delete the journal, we can just update the filename for each chunk to it's normal location

      # this commits the compaction
      journal_file.delete if journal_file.exists?


      # move all the compacted files back into position
      base_path=File.dirname(journal_filename)
      Dir.glob(File.join(compaction_working_path,"*#{CHUNK_EXT}")).each do |compacted_file|
        chunk_file = File.join(base_path,File.basename(compacted_file))
        # TODO: lock chunk_file's matching DiskChunk object
        FileUtils.rm [chunk_file] if File.exists?(chunk_file)
        FileUtils.mv compacted_file,chunk_file
        # TODO: reset and then unlock chunk_file's matching DiskChunk object
        # PLAN: implement a global has of chunk filenames => chunk objects. Only ever have at most one in memory chunk object per chunk file.
        #   Then we can just:
        #     Chunk[chunk_file].reset
        (cf = DiskChunk[chunk_file]) && cf.reset
      end

      FileUtils.rm successful_compaction_filename

      # remove compaction_working_path
      Dir.rmdir compaction_working_path
    end
  end
end
