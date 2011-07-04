# encoding: BINARY
require "fileutils"
require 'digest/md5'
=begin
This implementation of DiskChunk loads all the leaf IndexBlocks and fully populates @records.
  Fields are loaded on demand.
  Writes are journaled.
=end

module MonoTable

  class DiskChunk < DiskChunkBase
    def parse(io_stream)
      parse_base(io_stream)
      # parse the index-block, and optionally, load the data
      @data_loaded = false
      @records = parse_index_block(io_stream,DiskRecord)
    end
  end
end
