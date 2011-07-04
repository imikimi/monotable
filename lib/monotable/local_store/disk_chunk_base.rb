# encoding: BINARY
require "fileutils"
require 'digest/md5'

module MonoTable

  class DiskChunkBase < Chunk
    attr_accessor :file_handle
    attr_accessor :path_store
    attr_accessor :journal
    attr_accessor :max_chunk_size

    def initialize(filename,options={})
      init_disk_chunk_base(filename,options)
    end

    def init_disk_chunk_base(filename,options={})
      init_entry
      @file_handle=FileHandle.new(filename)
      @max_chunk_size = options[:max_chunk_size] || DEFAULT_MAX_CHUNK_SIZE
      @path_store=options[:path_store]
      @journal=options[:journal] || (path_store && path_store.journal) || Journal.new(filename+".testing_journal")

      # parse the file on disk
      # it is legal for the file on disk to not exist - which is equivelent to saying the chunk starts out empty. All writes go to the journal anyway and the file will be created when compaction occures.
      file_handle.read {|f|parse(f)} if file_handle.exists?
    end

    def filename
      @file_handle.filename
    end

    def data_loaded; false; end

    #*************************************************************
    # Write API
    #*************************************************************
    # NOTE: The "update" method inherited from Chunk works. No need to re-implement.
    def set(key,columns)
      ret=set_internal(key,journal.set(file_handle,key,columns))
      EventQueue<<ChunkFullEvent.new(self) if accounting_size > max_chunk_size
      ret
    end

    def delete(key)
      journal.delete(file_handle,key)
      delete_internal(key)
    end

    #*************************************************************
    # Parsing
    #*************************************************************
    def parse_base(io_stream)
      # convert String to StringIO
      io_stream = StringIO.new(io_stream) if io_stream.kind_of?(String)

      # parse header
      parse_header(io_stream)

      # skip over the checksum
      checksum = io_stream.read_asi_string
      entry_length = io_stream.read_asi

      # load the info-block
      parse_info_block(io_stream)

      # load the columns-block
      parse_columns_block(io_stream)

      # load the index-block
      index_block_length = io_stream.read_asi
      @data_block_offset = io_stream.pos + index_block_length
    end
  end
end
