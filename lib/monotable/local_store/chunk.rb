# encoding: BINARY
require 'digest/md5'

module MonoTable

  class Chunk < Entry

    # from_s can be a string or IOStream to the raw bytes of a chunk (one or more entries)
    # OR it can be an Entry object
    def initialize(from_s=nil)
      if from_s.kind_of? Entry
        init_entry(from_s)
      else
        init_entry
        if from_s
          io_stream = from_s.kind_of?(String) ? StringIO.new(from_s) : from_s
          parse(io_stream)
        end
      end
    end

    def Chunk.load(filename)
      File.open(filename,"rb") {|f|Chunk.new(f)}
    end

    #################################
    # bulk edits
    #################################
    def split(on_key)
      ret=split_into(on_key,Chunk.new)
      update_accounting_size
      ret.update_accounting_size
      ret
    end
  end
end
