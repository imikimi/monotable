require "fileutils"
=begin
NOT THREADSAFE
  Journals will not be threadsafe. Instead, JournalManagers will guarantee serial access to Journals
=end

module Monotable
  class Journal
    class << self
      # set Journal.async_compaction=true to enable asynchronous compaction
      attr_accessor :async_compaction
    end
    attr_accessor :journal_file
    attr_accessor :read_only
    attr_accessor :size
    attr_accessor :journal_manager
    attr_accessor :max_journal_size

    def initialize(file_name,options={})
      @journal_manager=options[:journal_manager]
      @journal_file=FileHandle.new(file_name.chomp(COMPACT_DIR_EXT))
      @read_only=@journal_file.exists?
      @size=@journal_file.length
      @max_journal_size = options[:max_journal_size] || DEFAULT_MAX_JOURNAL_SIZE
    end

    def full?
      @size > @max_journal_size
    end

    def read_entry(disk_offset,disk_length)
      journal_file.open_read(true)
      journal_file.read_handle.seek(disk_offset)
      Journal.parse_entry(journal_file.read_handle)
    end

    def Journal.parse_entry(io_stream)
      entry_string=Monotable::Tools.read_asi_checksum_string_from_file(io_stream)
      io_stream = StringIO.new(entry_string)
      strings=[]
      strings<<io_stream.read_asi_string while !io_stream.eof?
      command = strings[0]
      case command
      when "del" then {:command=>:delete,:chunk_file=>strings[1],:key=>strings[2]}
      when "split" then {:command=>:split,:chunk_file=>strings[1],:key=>strings[2],:to_file => strings[3]}
      when "set" then
        fields={}
        i=3
        while i < strings.length
          fields[strings[i]] = strings[i+1]
          i+=2
        end
        {:command=>:set,:chunk_file=>strings[1],:key=>strings[2],:fields=>fields}
      else
        raise "invalid Journal Chunk command: #{command.inspect}"
      end
    end

    def Journal.apply_entry(journal_entry,chunk,chunks)
      case journal_entry[:command]
      when :set then chunk.set(journal_entry[:key],journal_entry[:fields])
      when :delete then chunk.delete(journal_entry[:key])
      when :split then
        chunk2=chunk.split(journal_entry[:key])
        chunks[journal_entry[:to_file]]={:chunk=>chunk2}
      end
    end

    def journal
      self
    end

    def save_entry(string_array)
      save_str=string_array.collect {|str| [str.length.to_asi,str]}.flatten.join
      journal_file.open_append(true)
      @size+=Monotable::Tools.write_asi_checksum_string(journal_file,save_str)
      journal_file.flush
      MiniEventMachine.queue {self.compact} if full?
      save_str
    end

    def set(chunk_file,key,record)
      offset=@size
      save_str=save_entry((["set",chunk_file.to_s,key] + record.collect {|k,v| [k,v]}).flatten)
      length=save_str.length
      JournalDiskRecord.new(key,self,offset,length,record)
    end

    def delete(chunk_file,key)
      save_entry ["del",chunk_file.to_s,key]
    end

    def split(chunk_file,key,to_filename)
      save_entry ["split",chunk_file.to_s,key,to_filename]
    end

    def Journal.each_entry(journal_file)
      journal_file.read(0,nil,true) do |io_stream|
        while !io_stream.eof?
          yield Journal.parse_entry(io_stream)
        end
      end
    end

    def Journal.compaction_dir(journal_filename)
      journal_filename.to_s + COMPACT_DIR_EXT
    end

    def Journal.successfile_compaction_filename(journal_filename)
      File.join Journal.compaction_dir(journal_filename), JOURNAL_COMPACTION_SUCCESS_FILENAME
    end

    # file is a FileHandle or filename
    # phase 1 does 99% of the work:
    #   * reads the journal
    #   * reads all the effected chunks
    #   * generates the new versions of the chunks in a temporary directory.
    # Phase 1 can safely be run as long as the journal_file is no longer being written to.
    # It is safe to continue to read from the journal and the effected chunks during Phase 1.
    def Journal.compact_phase_1(journal_file)
      journal_file = FileHandle.new(journal_file) unless journal_file.kind_of?(FileHandle)
      compacted_chunks_path = Journal.compaction_dir(journal_file.to_s)
      success_filename = Journal.successfile_compaction_filename(journal_file)

      # test to see if this phase is already done
      return if Journal.compact_phase_1_succeeded(journal_file)

      chunks={}
      base_path=File.dirname(journal_file.filename)
      FileUtils.mkdir compacted_chunks_path unless File.exists?(compacted_chunks_path)

      if journal_file.exists?
        # apply journal to every chunk
        # NOTE: this loads all chunks that were touched fully into memory. This may be a problem :) - it may not fit in memory.
        #   However, I think the best way to solve this problem is to limit the number of chunks one journal manages, somewhere upstream.
        #   This would just mean we'd need to have more than one active journal if we have too many difference chunks being written to.
        # TODO: if the journal is somehow corrupt, I think the right answer is to stop parsting the journal, but keep all changes so far and continue.
        # The chunks -may- be OK if there were no further writes to them in the rest of the corrupt journal. We can detect their valididity later or immeidately with replicas on other disks.
        # Perhaps we need a possibly-corrupt directory which implies we need to compare with the replicas to verify integrity... ?

        each_entry(journal_file) do |entry|
          chunk_filename = entry[:chunk_file]
          if ch=chunks[chunk_filename]
            chunk=ch[:chunk]
          else
            chunk=MemoryChunk.load(chunk_filename)
            chunks[chunk_filename]={:chunk=>chunk}
          end
          Journal.apply_entry(entry,chunk,chunks)
        end
        journal_file.close

        # write all compacted chunks to disk
        # TODO: Write comacted_chunks to most-empty/least-loaded PathStores to Balance them
        chunks.each do |chunk_filename,status|
          chunk = status[:chunk]
          cf = status[:compacted_file] = File.join(compacted_chunks_path, File.basename(chunk_filename))
          chunk.save cf
        end

        # "touch" the file: JOURNAL_COMPACTION_SUCCESS_FILENAME
        File.open(success_filename,"w") {}
      end
    end

    # executes the compaction phase-1 as an external processes
    def Journal.compact_phase_1_external(journal_file)
      journal_file = FileHandle.new(journal_file) unless journal_file.kind_of?(FileHandle)
      # test to see if there is actually any phase-1 work
      return if Journal.compact_phase_1_succeeded(journal_file)

      pid=nil
      ret=nil
      binary=File.join File.dirname(__FILE__), "../../../bin", "compact.rb"
      command="#{binary} #{journal_file}"
      begin
        IO.popen(command) do |pipe|
          pid=pipe.pid
          ret=pipe.read
        end
      ensure
        Process.detach pid if pid
      end
      raise "Journal.compact_phase_1_external failed (ret=#{ret.inspect})" unless ret.strip=="SUCCESS"
      true
    end

    def Journal.compact_phase_1_succeeded(journal_file)
      success_filename = Journal.successfile_compaction_filename(journal_file)
      compacted_chunks_path = Journal.compaction_dir(journal_file.to_s)
      (!journal_file.exists? && !File.exists?(compacted_chunks_path)) || File.exists?(success_filename)
    end

    # file can be a filename, or any object that to_s outputs a filename (e.g. FileHandle)
    # Phase 2 is the cleanup phase:
    #   * deletes old versions of chunks and moves the new versions in place
    #   * deletes the journal file
    #   * removes compacted_chunks_path
    # Exclusive locks on each chunk must be aquired as its old version is deleted and new version is moved in place
    # TODO: Chunks effected should be "reset" after compaction
    def Journal.compact_phase_2(journal_file)
      journal_file = FileHandle.new(journal_file) unless journal_file.kind_of?(FileHandle)
      compacted_chunks_path = Journal.compaction_dir(journal_file.to_s)
      success_filename = Journal.successfile_compaction_filename(journal_file)

      # check to see if there is anything to do
      return unless journal_file.exists? || File.exists?(compacted_chunks_path)

      # verify the successfile exists
      raise "compact_phase_1 did not complete" unless File.exists?(success_filename)

      # TODO: lock all the effected chunks until the end of this block
      #   Better-yet, as we save each chunk, we should update its in-memory DiskChunk.
      #   This will point to the file in the compacted_chunk_path, though.
      #   And then after we delete the journal, we can just update the filename for each chunk to it's normal location

      # this commits the compaction
      journal_file.delete if journal_file.exists?


      # move all the compacted files back into position
      base_path=File.dirname(journal_file.filename)
      Dir.glob(File.join(compacted_chunks_path,"*#{CHUNK_EXT}")).each do |compacted_file|
        chunk_file=File.join(base_path,File.basename(compacted_file))
        # TODO: lock chunk_file's matching DiskChunk object
        FileUtils.rm [chunk_file] if File.exists?(chunk_file)
        FileUtils.mv compacted_file,chunk_file
        # TODO: reset and then unlock chunk_file's matching DiskChunk object
        # PLAN: implement a global has of chunk filenames => chunk objects. Only ever have at most one in memory chunk object per chunk file.
        #   Then we can just:
        #     Chunk[chunk_file].reset
        (cf=DiskChunk[chunk_file]) && cf.reset
      end

      FileUtils.rm success_filename

      # remove compacted_chunks_path
      Dir.rmdir compacted_chunks_path
    end

    # compact this journal and all the chunks it is tied to
    # the end result is the journal is deleted and all the chunk files have been updated
    # Contains auto-recovery code. If it crashes at any point, just run it again and it will recover assuming the failure was temporary (like a machine crash).
    # TODO: This assumes the journal and chunk files are not corrupt, if they
    #   are corrupt, we do not currently handle that. Corruption is currently being detected
    #   via checksums resulting, currently, in exceptions being thrown out of this
    #   method. We should trap these exceptions, and re-replicate from other servers where necessary.
    # TODO: This does not currently handle multitasking where the current in-memory copies of the chunks need to be updated safely
    # option:
    #   :async => true
    #     if async is true, then the phase_1 compaction is run externally and control is returned immediately.
    #     Be sure to run CompactionManager.singleton.process_queue at some later point to finalize journal processing.
    def compact(options={})
      journal_manager && journal_manager.freeze_journal(self)
      @read_only=true
      if Journal.async_compaction #options[:async]
        CompactionManager.compact(journal_file)
      else
        Journal.compact_phase_1(journal_file)
        Journal.compact_phase_2(journal_file)
      end
    end
  end
end

