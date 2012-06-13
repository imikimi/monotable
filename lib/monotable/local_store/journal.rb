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

      def encode_journal_entry(command, args)
        [command,args].flatten.collect {|str| [str.length.to_asi,str]}.flatten.join
      end

      # parses exactly the data encode_journal_entry generates
      def parse_encoded_journal_entry(encoded_journal_entry_stream)
        command = encoded_journal_entry_stream.read_asi_string
        strings = []
        strings << encoded_journal_entry_stream.read_asi_string while !encoded_journal_entry_stream.eof?
        {:command => command.to_sym}.merge(case command
          when "delete"       then {:key=>strings[0]}
          when "delete_chunk" then {}
          when "split"        then {:on_key => strings[0], :to_basename => strings[1]}
          when "move_chunk"   then {:to_store_path => strings[0]}
          when "set"          then {:key => strings[0], :fields => Hash[*strings[1..-1]]}
          else raise InternalError.new "parse_entry: invalid journal entry command: #{command.inspect}"
        end)
      end

      # Note, the "encoded_journal_entry" is wrapped in an asi_checksum and is prefixed by
      #   the chunk's basename asi_string.
      # parse_entry knows how to decode everything
      def parse_entry(io_stream)
        entry_string=Monotable::Tools.read_asi_checksum_string_from_file(io_stream)
        io_stream = StringIO.new(entry_string)
        chunk_basename = io_stream.read_asi_string
        parse_encoded_journal_entry(io_stream).merge(:chunk_basename => chunk_basename)
      end
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

    def journal
      self
    end

    def local_store
      @local_store ||= journal_manager && journal_manager.local_store
    end

    # Writes the encoded_journal_entry to disk.
    # encoded_journal_entry is prepended with the chunk.basename string.
    #   chunk.basename is used instead of chunk.range_start because it allows the compactor to find the chunk's file on disk trivially rather having to scan all chunks on disk.
    # The data written to disk is also checksummed.
    def journal_write(chunk, encoded_journal_entry)
      data_to_write = [chunk.basename.length.to_asi, chunk.basename, encoded_journal_entry].join
      offset = @size
      journal_file.open_append true
      @size += Monotable::Tools.write_asi_checksum_string journal_file, data_to_write
      journal_file.flush
      EM::next_tick {self.compact} if full?
      {:offset => offset, :length => data_to_write.length, :journal => self}
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
    def compact(options={},&block)
      journal_manager && journal_manager.freeze_journal(self)
      @read_only=true

      if Journal.async_compaction #options[:async]
        CompactionManager.compact(journal_file, :local_store => local_store, &block)
      else
        compactor = Compactor.new(journal_file, :local_store => local_store)
        compactor.compact_phase_1
        compactor.compact_phase_2
        yield if block
      end
    end
  end
end
