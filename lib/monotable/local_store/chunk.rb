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
        parse_all_entries(from_s) if from_s
      end
    end

    def Chunk.load(filename)
      chunk=File.open(filename,"rb") {|f|Chunk.new(f)}
#      journal_filename=filename+".journal"
#      File.open(journal_filename,"rb") {|f| chunk.parse_all_entries(f)} if File.exists?(journal_filename)
      chunk
    end

    #################################
    # bulk edits
    #################################
    def split(on_key)
      split_into(on_key,Chunk.new)
    end

    ################################
    # multi-entry parsing
    ################################

    def parse_all_entries(io_stream)
      io_stream = StringIO.new(io_stream) if io_stream.kind_of?(String)
      while !io_stream.eof?
        entry = Entry.new(io_stream)
        apply_entry(entry)
      end
    end
  end
end
