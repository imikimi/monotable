require "fileutils"
=begin
NOT THREADSAFE
  Journals will not be threadsafe. Instead, JournalManagers will guarantee serial access to Journals
=end

module MonoTable
  class Journal
    attr_accessor :journal_file
    attr_accessor :read_only
    attr_accessor :size
    attr_accessor :journal_manager
    attr_accessor :max_journal_size

    def initialize(file_name,options={})
      @journal_manager=options[:journal_manager]
      @journal_file=FileHandle.new(file_name.chomp(COMPACT_DIR_EXT))
      @read_only=journal_file.exists?
      @size=journal_file.length
      @max_journal_size = options[:max_journal_size] || DEFAULT_MAX_JOURNAL_SIZE
      puts "max_journal_size=#{max_journal_size}"
    end

    def full?
      @size > @max_journal_size
    end

    def hold_file_open
      journal_file.open_append
    end

    def close_file
      journal_file.close
    end

    def Journal.read_entry(file)
      checksum_string=MonoTable::Tools.read_asi_checksum_string_from_file(file)
      file = StringIO.new(checksum_string)
      strings=[]
      strings<<file.read_asi_string while !file.eof?
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

        # TODO - there is no reason to actually write this to disk here. We will be writing to disk again after the journal compaction
        chunk2.save(journal_entry[:to_file])
        chunks[journal_entry[:to_file]]={:chunk=>chunk2}
      end
    end

    def journal
      self
    end

    def save_entry(string_array)
      save_str=string_array.collect {|str| [str.length.to_asi,str]}.flatten.join
      save_str=MonoTable::Tools.to_asi_checksum_string(save_str)
      journal_file.append save_str
      journal_file.flush
      @size+=save_str.length
      EventQueue << JournalFullEvent.new(self) if full?
      save_str
    end

    def set(chunk_file,key,record)
      offset=@size
      save_str=save_entry (["set",chunk_file.to_s,key] + record.collect {|k,v| [k,v]}).flatten
      length=save_str.length
      JournalDiskRecord.new(key,journal_file,offset,length,record)
    end

    def delete(chunk_file,key)
      save_entry ["del",chunk_file.to_s,key]
    end

    def split(chunk_file,key,to_filename)
      save_entry ["split",chunk_file.to_s,key,to_filename]
    end

    # writes the entry to disk and then converts all records in the entry to DiskRecords that line-up with the just-written data
    def append(entry)
      raise "depricated"
      raise "Journal is read-only" if @read_only
      index={}
      write_data=entry.to_binary(index)
      offset=size
      journal_file.append write_data
      @size+=write_data.length
      entry.records=index
      entry.records.each {|k,record| record.match_to_entry_on_disk(offset,journal_file,entry.columns)}
      entry
    end

    def each_entry
      journal_file.read do |file|
        while !file.eof?
          yield Journal.read_entry(file)
        end
      end
      journal_file.open_write
    end

    # compact this journal and all the chunks it is tied to
    # the end result is the journal is deleted and all the chunk files have been updated
    # Contains auto-recovery code. If it crashes at any point, just run it again and it will recover assuming the failure was temporary (like a machine crash).
    # TODO: This assumes the journal and chunk files are not corrupt, if they
    #   are corrupt, we do not currently handle that. Corruption is currently being detected
    #   via checksums resulting, currently, in exceptions being thrown out of this
    #   method. We should trap these exceptions, and re-replicate from other servers where necessary.
    # TODO: This does not currently handle multitasking where the current in-memory copies of the chunks need to be updated safely
    def compact
      journal_manager && journal_manager.freeze_journal(self)
      @read_only=true
      chunks={}
      compacted_chunks_path = journal_file.filename + COMPACT_DIR_EXT
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

        each_entry do |entry|
          chunk_filename = entry[:chunk_file]
          if ch=chunks[chunk_filename]
            chunk=ch[:chunk]
          else
            chunk=MemoryChunk.load(chunk_filename)
            chunks[chunk_filename]={:chunk=>chunk}
          end
          Journal.apply_entry(entry,chunk,chunks)
        end

        # write all compacted chunks to disk
        # TODO: Write comacted_chunks to most-empty/least-loaded PathStores to Balance them
        chunks.each do |chunk_filename,status|
          chunk = status[:chunk]
          cf = status[:compacted_file] = File.join(compacted_chunks_path, File.basename(chunk_filename))
          chunk.save cf
        end

        # TODO: lock all the effected chunks until the end of this block
        #   Better-yet, as we save each chunk, we should update its in-memory DiskChunk.
        #   This will point to the file in the compacted_chunk_path, though.
        #   And then after we delete the journal, we can just update the filename for each chunk to it's normal location

        # this commits the compaction
        journal_file.delete
      end

      # move all the compacted files back into position
      Dir.glob(File.join(compacted_chunks_path,"*#{CHUNK_EXT}")).each do |compacted_file|
        chunk_file=File.join(base_path,File.basename(compacted_file))
        # TODO: lock chunk_file's matching DiskChunk object
        FileUtils.rm [chunk_file]
        FileUtils.mv compacted_file,chunk_file
        # TODO: reset and then unlock chunk_file's matching DiskChunk object
      end


      # remove compacted_chunks_path
      Dir.rmdir compacted_chunks_path
    end
  end
end

