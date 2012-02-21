# encoding: BINARY
require "fileutils"
require 'digest/md5'

=begin
This implementation of DiskChunk only loads the first IndexBlock when created. Other IndexBlocks are loaded on demand.
  Sub-IndexBlocks, DiskRecords and fields are loaded on demand.
  Writes are journaled.

DiskChunk's record's structure consists of three sub-structures:

  @tip_index_block
    Index & Records on disk

  @deleted_records
    The value of @deleted_records[key].key is the Record object representing the record on disk. Therefor:
      @deleted_records[key].key == key
    records which exist in the chunk on disk, but are logged to be deleted in the journal and conseqently WILL be deleted on the next compaction
    A record will be in @deleted_records iff:
      a) that Key-Record exists on disk AND
      b) the record has been Deleted, Set or Updated after the most recent compaction

  @records
    records which:
      a) do NOT exist in the chunk on disk, but are logged to be created in the journal
      b) OR do exists, but have been updated or overwritten - in this case there will be a matching entry in @deleted_records
    NOTE: if a Key is in both @deleted_records AND @records, this means:
      a) the record exists on disk
      b) the record is NOT deleted
      c) AND @records (and the journal file) holds the up-to-date version
=end

module Monotable

  class DiskChunk < DiskChunkBase
    attr_accessor :index_level_offsets
    attr_accessor :index_level_lengths
    attr_accessor :top_index_block

    # options
    #   :filename =>
    def initialize(options={})
      @deleted_records={}
      init_disk_chunk_base(options)
    end

    # returns number of records in the chunk
    def length; @loaded_record_count - @deleted_records.length + @records.length; end

    # returns a list of all keys in the chunk (unsorted)
    # this is very inefficient - it has to load the entire index into memory, but there is no other way to do it.
    # Just don't use this for any real work ;).
    def keys
      keys=[]
      each_key_unsorted {|key| keys<<key}
      keys
    end

    #***************************************************
    # iterators
    #***************************************************
    # yields each key in the chunk in sorted order
    def each_key
      each_record {|r| yield r.key}
    end

    # yields each key in the chunk, not necessarilly in sorted order
    # this is (potentially) faster than each_key
    def each_key_unsorted
      @records.each {|key,value| yield key}
      (@top_index_block||[]).each {|key,record| yield key unless @deleted_records[key]}
    end

    # yields every record in the chunk in sorted Key-order
    def each_record
      # if each_record is used much, we should store @records in a sorted data structure to avoid the .sort
      mem_record_keys = @records.keys.sort
      mrk_index = 0

      # syncronized, step through both @records (sorted by key) and @top_index_block.each
      # skips records in @deleted_records
      (@top_index_block||[]).each do |disk_key,record|

        # yield all memory records before "record"
        while (key=mem_record_keys[mrk_index]) && key < disk_key
          yield @records[key]
          mrk_index+=1
        end

        # yield "record" unless it is deleted
        yield record unless @deleted_records[disk_key]
      end

      # yield any remaining memory records
      while(key=mem_record_keys[mrk_index]) do
        yield @records[key]
        mrk_index+=1
      end
    end

    #***************************************************
    # IndexBlock interface compatibility
    #***************************************************
    # provided for compatibility so DiskChunk object can be the "parent" of an IndexBlock
    def chunk; self; end
    def index_depth; -1; end

    #*************************************************************
    # read API
    #*************************************************************

    # see ReadAPI
    def get_record(key)
      @records[key] || (!@deleted_records[key] && locate_index_record(key))
    end

    #*************************************************************
    # Write API
    #*************************************************************

    # see WriteAPI
    def set(key,columns)
      @deleted_records[key]=locate_index_record(key) if exists_on_disk?(key)
      super
    end

    # see WriteAPI
    def delete(key)
      @deleted_records[key]=locate_index_record(key) if exists_on_disk?(key)
      super
    end

    #***************************************************
    # parsing
    #***************************************************

    # Loads just the top index-block from the stream.
    # Initializes just enough so we can load the other index-blocks on demand.
    def partially_parse_index(io_stream)
      # load index level
      num_index_levels=io_stream.read_asi
      @index_level_lengths=[]
      num_index_levels.times {@index_level_lengths<<io_stream.read_asi}

      # calculate the level_offsets
      cur_offset=io_stream.pos
      @index_level_offsets=[]
      @index_level_lengths.each do |length|
        @index_level_offsets<<cur_offset
        cur_offset+=length
      end

      # load the first block
      @top_index_block = IndexBlock.new(self,"",0,@index_level_lengths[0],:io_stream=>io_stream)
    end

    # Parses the chunk-file from byte-0 on.
    # This parser doesn't load the whole file.
    # Only the first headers and the first index-block are loaded.
    # Sub-index-blocks and records are loaded on demand.
    #
    # This replaces DiskChunkBase#parse which loads the entire chunk into memory.
    #
    # initializes @records
    def parse(io_stream)
      parse_base(io_stream)
      partially_parse_index(io_stream)
      @records = {}
    end

    #*************************************************************
    # additional useful internal API
    #*************************************************************

    def locate_index_record(key)
      @top_index_block && @top_index_block.locate(key)
    end

    def exists_on_disk?(key)
      locate_index_record(key) && true
    end

    def exists?(key)
      (@records[key] || (!@deleted_records[key] && locate_index_record(key))) && true
    end

=begin

Thoughts in a more efficient middle-key algorithm:

  NOTE: for a given key, we can calc size1 and size2 by combining the info from the 2 in-memory structures and only reading
  O(log(n)) index-blocks from disk.

  If we convert @records and @deleted_records into arrays and sort them on their keys, as we recurse in on the O(log(n)) index blocks,
  we can maintain an accurate size1/size2 number pair and we can choose what block to recurse to such that we know for certain that the
  accurately optimal middle-key is contained in that block.

  We can start by scanning @records and @deleted records and totaling accounting_sizes until we match with the index-record we are examining in
  then on-disk data structure. Then, we can scan back and forwards within @records and @deleted while maintaining their totals to maintain
  an accurate grand total.

  Actually, I think we only need to maintain one grand-total.

  NOTE: this does mean we have to scan the in-memory @records and @deleted_records. That's approximately O(n/2) work where n = @records.length + @deleted_records.length.
  This could be improved with some sort of tree structure that is maintained as @records and @deleted_records are built up. However, this
  may be a fair amount of complexity and constant-time overhead. It is hard to say if it would be a net-win. "n" is ultimiately limited by the chunk-size.

=end
  end
end
