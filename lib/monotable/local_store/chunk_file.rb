# encoding: BINARY
require "fileutils"
require 'digest/md5'

module MonoTable

  class ChunkFile < DiskChunkBase
    def parse(io_stream)
      parse_base(io_stream)
      # parse the index-block, and optionally, load the data
      @data_loaded = false
      @records = parse_index_block(io_stream,DiskRecord)
    end
  end
end
