# encoding: BINARY
require 'digest/md5'
=begin
This sub-class of Chunk loads the entier chunk into memory, or can work without a disk-image at all.
Write methods only alter the in-memory structure.
=end

module Monotable

  class MemoryChunk < Chunk

    # from_s can be a string or IOStream to the raw bytes of a chunk (one or more entries)
    # OR it can be an Chunk object
    # options
    #   :data => string
    def initialize(options={})
      data = options[:data]
      init_chunk(options)
      data ||= file_handle && file_handle.read
      if data
        io_stream = data.kind_of?(String) ? StringIO.new(data) : data
        parse(io_stream)
      end
    end

    def MemoryChunk.load(filename)
      MemoryChunk.new(:filename => filename)
    end

    #***************************************************
    # split
    #***************************************************
    def split_simple(options={})
      Tools.debug options
      options[:new_chunk] ||= self.clone
      super
    end

    #***************************************************
    # parsing
    #***************************************************
    # io_stream can be a String or anything that supports the IO interface
    def parse(io_stream)
      # convert String to StringIO
      io_stream = StringIO.new(io_stream) if io_stream.kind_of?(String)

      # parse header
      parse_header(io_stream)

      # parse the checksum
      # load the entire entry and validate the checksum
      entry_body,ignored_index = Tools.read_asi_checksum_string(io_stream)

      # resume parsing from the now-loaded entry_body
      io_stream = StringIO.new(entry_body)

      # load the info-block
      parse_info_block(io_stream)

      # load the columns-block
      parse_columns_block(io_stream)

      # parse the index-block
      index_block_length=io_stream.read_asi
      @records = parse_index_block(io_stream,MemoryRecord)

      # data_block
      data_block = io_stream.read

      # init the records from the index-records
      @data_loaded = true
      @records.each do |k,record|
        record.parse_record(data_block[record.disk_offset,record.disk_length],@columns)
      end
    end

  end
end
