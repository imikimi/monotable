module MonoTable

  class IndexBlock
    attr_accessor :index_records,:depth,:level_offsets,:level_lengths,:root_key,:file_handle,:offset,:length,:leaf
    attr_accessor :chunk

    def initialize(chunk,depth,level_offsets,level_lengths,root_key,file_handle,io_stream=nil)
      @chunk=chunk
      @depth=depth
      @level_offsets=level_offsets
      @level_lengths=level_lengths
      @root_key=root_key
      @offset=level_offsets[depth]
      @length=level_lengths[depth]
      @leaf = depth == level_lengths.length-1

      #if there is an existing io_stream, use it, otherwise use the file_handle
      if io_stream
        parse(io_stream,length)
      else
        file_handle.read(@offset) do |io_stream|
          parse(io_stream,length)
        end
      end
    end

    def leaf?; @leaf; end

    def parse(io_stream,block_length)
      last_key=@root_key
      end_pos=io_stream.pos + block_length
      @index_records=RBTree.new
      while io_stream.pos < end_pos #io_stream.eof?
        ir=DiskRecord.new(chunk,0).parse_index_record(io_stream,last_key)
#        ir=IndexRecord.parse(io_stream,last_key)
        last_key=ir.key
        @index_records[ir.key]=ir
      end
      @index_records
    end

    def sub_index_block(index_record)
      # currenly I'm keeping in memory every index-block read
      # in the future this should go through a central cache that only keeps in memory a fixed number of most-recently-used blocks
      index_record.sub_index_block||=IndexBlock.new(chunk,depth+1,level_offsets,level_lengths,index_record.key,file_handle)
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
        @index_records.each do |ir|
          ir.sub_index_block||=IndexBlock.new(chunk,depth+1,level_offsets,level_lengths,ir.key,file_handle)
          ir.sub_index_block.each(&block)
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
