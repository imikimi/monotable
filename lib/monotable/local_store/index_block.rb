module Monotable

  class IndexBlock
    # core attributes
    attr_accessor :root_key     # All keys in this block are >= this key
    attr_accessor :disk_offset  # absolute disk_offset from start of file
    attr_accessor :disk_length  # byte-disk_length of this block on disk
    attr_accessor :parent       # either an IndexBlock or the DiskChunk itself if this is the root IndexBlock

    # cached/derived from "parent"
    attr_accessor :index_depth  # 0 == root IndexBlock
    attr_accessor :leaf         # true if index_depth == chunk.index_level_offsets.disk_length-1
    attr_accessor :file_handle
    attr_accessor :chunk

    attr_accessor :index_records

    # parent should be a DiskChunk or an Indexblock
    # options:
    #   :io_stream - if there is an open IO stream already pointing exactly at the index-block, this is the efficient way to read it
    def initialize(parent,root_key,disk_offset,disk_length,options={})
      @parent=parent
      @chunk = parent.chunk
      @file_handle = @chunk.file_handle
      @index_depth = parent.index_depth+1
      @leaf = index_depth == index_level_offsets.length-1

      @root_key = root_key
      @disk_offset = disk_offset #+ index_level_offsets[@index_depth]
      @disk_length = disk_length

      #if there is an existing io_stream, use it, otherwise use the file_handle
      io_stream=options[:io_stream]
      if io_stream
        parse(io_stream,disk_length)
      else
        file_handle.read(@disk_offset) do |io_stream|
          parse(io_stream,disk_length)
        end
      end
    end

    def memory_size
      @disk_length
    end

    def index_level_offsets; chunk.index_level_offsets; end
    def index_level_lengths; chunk.index_level_lengths; end

    def inspect
      "<IndexBlock root_key=#{root_key.inspect} index_depth=#{index_depth} disk_offset=#{disk_offset} disk_length=#{disk_length}/>"
    end

    def leaf?; @leaf; end

    def parse(io_stream,block_length)
      last_key=@root_key
      end_pos=io_stream.pos + block_length
      @index_records=RBTree.new
      offset_base = leaf? ? chunk.data_block_offset : index_level_offsets[@index_depth+1]
      while io_stream.pos < end_pos #io_stream.eof?
        ir=DiskRecord.new(chunk,offset_base).parse_index_record(io_stream,last_key)
        last_key=ir.key
        @index_records[ir.key]=ir
      end
      @index_records
    end

    def cache
      @cache ||= Cache.global_cache
    end

    def sub_index_block(index_record)
      key=index_record.sub_block_key
      key<<@index_depth

      # Caching note: we need to reset the cache at least for each separate test run (test are failing)
      # How do we want to handle more than one local-store active at a time? In general, this shouldn't happen, but it is good to avoid globals anyway
      # perhaps the "global_cache" should be localized to a local-store.
      # Last, we need to think through how the cache gets reset post compaction when we reset a chunk. We need some way to bulk invalidate entries.
      # Gut: have special memory rev-num for the chunk which get incremented with each compaction. Add that to the key. Let automatic eviction delete the old, unused entries
      cache.get(key) do
        IndexBlock.new(self,index_record.key,index_record.disk_offset,index_record.disk_length)
      end
    end

    def locate(key)
      # upper_bound returns match with key <= the passed in key
      match_key,index_record=@index_records.upper_bound(key)
      return nil unless match_key
      return match_key==key ? index_record : nil if leaf

      sub_index_block(index_record).locate(key)
    end

    def each(&block)
      if @leaf
        @index_records.each(&block)
      else
        @index_records.each do |key,ir|
          sub_index_block(ir).each(&block)
        end
      end
    end

    # returns the key <= the middle record as measured by accounting size
    # [record, accounting_offset of record]
    def middle_key_lower_bound(base_accounting_offset,middle_offset)
      index_records.each_with_index do |record,i|
        accounting_size=record.accounting_size
        if base_accounting_offset + accounting_size > middle_offset
          if leaf?
            return [record,base_accounting_offset]
          else
            return sub_index_block(record).middle_key_lower_bound(base_accounting_offset,middle_offset)
          end
        end
        base_accounting_offset+=accounting_size
      end
    end
  end
end
