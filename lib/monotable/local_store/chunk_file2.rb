# encoding: BINARY
require "fileutils"
require 'digest/md5'

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

    def locate(key)
      # upper_bound returns match with key <= the passed in key
      match_key,index_record=@index_records.upper_bound(key)
      return nil unless match_key
      return match_key==key ? index_record : nil if leaf

      # currenly I'm keeping in memory every index-block read
      # in the future this should go through a central cache that only keeps in memory a fixed number of most-recently-used blocks
      index_record.sub_index_block||=IndexBlock.new(chunk,depth+1,level_offsets,level_lengths,match_key,file_handle)
      sub_index_block.locate(key)
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
  end

  class ChunkFile < Entry
    attr_accessor :file_handle
    attr_accessor :path_store
    attr_accessor :journal
    attr_accessor :max_chunk_size

    def initialize(fn,options={})
      init_entry
      self.file_handle=FileHandle.new(fn)
      @max_chunk_size = options[:max_chunk_size] || DEFAULT_MAX_CHUNK_SIZE
      @path_store=options[:path_store]
      @journal=options[:journal] || (path_store && path_store.journal) || Journal.new(fn+".testing_journal")

      @deleted_records={}
      load_index_block
    end

    def [](key)
      get(key)
    end

    def filename
      @file_handle.filename
    end

    def length; @loaded_record_count - @deleted_records.length + @records.length; end

    # this is very inefficient - it has to load the entire index into memory, but there is no other way to do it.
    # Just don't use this for any real work ;).
    def keys
      keys=[]
      @top_index_block.each {|key,ir| keys<<key}
      keys
    end

    #***************************************************
    # parsing
    #***************************************************
    def load_index_block
      return unless file_handle.exists? # it is legal for the file on disk to not exist - which is equivelent to saying the chunk starts out empty. All writes go to the journal anyway and the file will be created when compaction occures.
      file_handle.read {|f|parse_minimally(f)}
    end

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
      @top_index_block = IndexBlock.new(self,0,@index_level_offsets,@index_level_lengths,"",file_handle,io_stream)
    end

    def parse_minimally(io_stream)
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
      @data_block_offset=io_stream.pos + index_block_length

      # parse the index-block, and optionally, load the data
      @data_loaded=false
      partially_parse_index(io_stream)
    end

    def fetch_record(key)
      return nil if @deleted_records[key]
      record=@records[key]
      return record if record

      index_record=locate_index_record(key)
      index_record && index_record.key==key && DiskRecord.new(self).init(
        key,
        @data_block_offset+index_record.disk_offset,
        index_record.disk_length,
        index_record.accounting_size,
        file_handle,@columns
      )
    end

    def locate_index_record(key)
      @top_index_block.locate(key)
    end

    def record(key)
      locate_index_record(key)
    end

    def exists_on_disk?(key)
      locate_index_record(key) && true
    end

    #*************************************************************
    # Read API
    #*************************************************************
    def get(key,columns=nil)
      (record=fetch_record(key)) && record && record.fields(columns)
    end

    def exists?(key)
      !@deleted_records[key] && locate_index_record(key) && true
    end

    #*************************************************************
    # Write API
    #*************************************************************
    # NOTE: The "update" method inherited from Entry works. No need to re-implement.
    def set(key,columns)
      @deleted_records[key]=locate_index_record(key) if exists?(key)
      ret=set_internal(key,journal.set(file_handle,key,columns))
      EventQueue<<ChunkFullEvent.new(self) if accounting_size > max_chunk_size
      ret
    end

    def delete(key)
      journal.delete(file_handle,key)
      @deleted_records[key]=locate_index_record(key) if exists_on_disk?(key)
      delete_internal(key)
    end

    #*************************************************************
    # Internal API
    #*************************************************************
    # all keys >= on_key are put into a new chunk
    def split(on_key=nil,to_filename=nil)
      if on_key
        size1,size2=split_on_key_sizes(on_key)
      else
        on_key,size1,size2=middle_key_and_sizes
      end
      to_filename||=path_store.generate_filename

      # create new chunk
      second_chunk_file=ChunkFile.new(to_filename,:journal=>journal,:max_chunk_size=>max_chunk_size)

      # do the actual split
      # NOTE: this just splits the in-memory Records. If they are DiskRecords, they will still point to the same file, which is correct for reading.
      self.split_into(on_key,second_chunk_file)

      # update the path_store (which will also update the local_store
      path_store.add(second_chunk_file) if path_store

      # set entry
      journal.split(file_handle,on_key,to_filename)

      # update sizes
      self.accounting_size=size1 || self.calculate_accounting_size
      second_chunk_file.accounting_size=size2 || second_chunk_file.calculate_accounting_size

      # return the new ChunkFile object
      second_chunk_file
    end

    # returns array: [sizes < on_key, sizes >= on_key]
    def split_on_key_sizes(on_key)
      size1=size2=0
      records.each do |k,v|
        asize=v.accounting_size
        if k < on_key
          size1+=asize
        else
          size2+=asize
        end
      end
      [size1,size2]
    end

=begin
Here's how I think we can do this without reading the entire index off disk.

We have 3 data structures we need to coordinate to get an accurate answer:

  Index on disk
  @deleted_records
    # @deleted_records[key] == the IndexRecord for that record on disk
    # records which exist in the chunk on disk, but are logged to be deleted in the journal and conseqently WILL be deleted on the next compaction
    # this will only be true for a given Key if that Key-Record exists and the record has been deleted, set or updated after the most recent compaction
  @records
    # records which:
    #   a) do NOT exist in the chunk on disk, but are logged to be created in the journal
    #   b) OR do exists, but have been updated or overwritten - in this case there will be a matching entry in @deleted_records
    # NOTE: if a Key is in both @deleted_records AND @records, this means it exists and @records holds the up-to-date version
  @accounting_size should be up-to-date taking into account all three data-structures

Possible algorithm:

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
    # returns array: [middle_key, sizes < middle_key, sizes >= middle_key]
    def middle_key_and_sizes_2
      mem_recs=@records.values.sort_by {|a| a.key}
      del_recs=@deleted_records.values.sort_by {|a| a.key}
      cur_index_block=@top_index_block
      total_lower_bound=0
      total_upper_bound=accounting_size
      half_size=accounting_size/2
    end

    # returns array: [middle_key, sizes < middle_key, sizes >= middle_key]
    def middle_key_and_sizes
      raise "not supported yet"
      half_size=accounting_size/2
      size1=size2=0
      mkey=nil

      # determine the middle-most key
      records.keys.sort.each do |key|
        v=records[key]
        asize=v.accounting_size
        if size1+(asize/2)>half_size
          mkey=key
          size2+=accounting_size-size1
          break
        end
        size1+=asize
      end
      # Guarantees:
      # if records.length > 0 then size1 is > 0
      # if records.length > 1 then size2 is also > 0
      # size1 + size2 == accounting_size
      [mkey,size1,size2]
    end
  end
end
