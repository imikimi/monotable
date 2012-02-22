# encoding: BINARY
require "fileutils"
require 'digest/md5'

module Monotable

  # DiskChunkBase will usually only be used as a superclass for DiskChunk.
  #
  # However, it can be used instead of DiskChunk. If functions identically externally.
  # Internally, it will load the entire chunk into memory on Init.
  # This is obviously much slower, but it is simpler and useful for testing.
  class DiskChunkBase < Chunk
    attr_accessor :file_handle
    attr_accessor :path_store
    attr_accessor :journal

    class << self
      def reset_disk_chunks
        @disk_chunks={}
      end

      def debug_chunks(info=nil)
        puts "DiskChunkBase(#{info.inspect}).disk_chunks: #{(@disk_chunks && @disk_chunks.keys).inspect}"
      end

      def disk_chunks
        @disk_chunks||={}
      end

      def [](chunk_file)
        chunk_file=File.expand_path(chunk_file) # normalize
        disk_chunks[chunk_file]
      end

      def []=(chunk_file,chunk)
        chunk_file=File.expand_path(chunk_file) # normalize
        raise "chunk already created" if disk_chunks[chunk_file]
        disk_chunks[chunk_file]=chunk
      end

      def init(options)
        self[options[:filename]]=
        DiskChunk.new(options)
      end
    end

    def initialize(options={})
      init_disk_chunk_base(options)
    end

    def reset
      super
      init_from_disk
    end

    # options
    #   :filename => required
    def init_disk_chunk_base(options={})
      raise ":filename require" unless options[:filename]
      init_chunk(options)
      @file_handle=FileHandle.new(options[:filename])
      @journal=options[:journal] || (path_store && path_store.journal) || Journal.new(options[:filename]+".testing_journal")

      init_from_disk
    end

    def init_from_disk
      # parse the file on disk
      # it is legal for the file on disk to not exist - which is equivelent to saying the chunk starts out empty. All writes go to the journal anyway and the file will be created when compaction occures.
      file_handle.read(0) {|f|parse(f)} if file_handle.exists?
    end

    def filename
      @file_handle.filename
    end

    def data_loaded; false; end

    def chunk_file_data
      @file_handle.read(0)
    end

    #*************************************************************
    # Write API
    #*************************************************************
    # NOTE: The "update" method inherited from Chunk works. No need to re-implement.

    # see WriteAPI
    def set(key,columns)
      ret=set_internal(key,journal.set(self,key,columns))
      MiniEventMachine.queue {self.split} if accounting_size > max_chunk_size
      ret
    end

    # see WriteAPI
    def delete(key)
      journal.delete(file_handle,key)
      delete_internal(key)
    end

    # delete this chunk
    # TODO: this should actually move the chunk into the "Trash" - a holding area where we can later do a verification against the cluster to make sure it is safe to delete this chunk.
    def delete_chunk
      journal.delete_chunk(file_handle)
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

    # Parses the chunk-file from byte-0 on.
    # This parser loads the entire file into memory.
    #
    # initializes @records
    def parse(io_stream)
      parse_base(io_stream)
      # parse the index-block, and optionally, load the data
      @data_loaded = false
      @records = parse_index_block(io_stream,DiskRecord)
    end
  end
end
