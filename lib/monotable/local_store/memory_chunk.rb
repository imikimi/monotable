# encoding: BINARY
require 'digest/md5'
=begin
This sub-class of Chunk loads the entier chunk into memory, or can work without a disk-image at all.
Write methods only alter the in-memory structure.
=end

module MonoTable

  class MemoryChunk < Chunk

    # from_s can be a string or IOStream to the raw bytes of a chunk (one or more entries)
    # OR it can be an Chunk object
    def initialize(from_s=nil)
      if from_s.kind_of? Chunk
        init_entry(from_s)
      else
        init_entry
        if from_s
          io_stream = from_s.kind_of?(String) ? StringIO.new(from_s) : from_s
          parse(io_stream)
        end
      end
    end

    def MemoryChunk.load(filename)
      File.open(filename,"rb") {|f|MemoryChunk.new(f)}
    end

    #***************************************************
    # split
    #***************************************************
    def split(on_key)
      ret=split_into(on_key,MemoryChunk.new)
      update_accounting_size
      ret.update_accounting_size
      ret
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
