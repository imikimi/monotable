# encoding: BINARY
require "fileutils"
require 'digest/md5'

module MonoTable

  class ChunkFile < Entry
    attr_accessor :file_handle
    attr_accessor :path_store
    attr_accessor :journal
    attr_accessor :max_chunk_size
    attr_accessor :index_level_offsets
    attr_accessor :index_level_lengths

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

    # returns number of records in the chunk
    def length; @loaded_record_count - @deleted_records.length + @records.length; end

    # returns a list of all keys in the chunk (unsorted)
    # this is very inefficient - it has to load the entire index into memory, but there is no other way to do it.
    # Just don't use this for any real work ;).
    def keys
      keys=@records
      (@top_index_block||[]).each {|key,ir| keys[key]=true}
      @deleted_records.each {|key,v| keys.delete_at(key)}
      keys.keys
    end

    # yields each key in the chunk
    # Unless the chunk has not been written to since it was opened, the order will be unsorted
    def each_key
      @records.each {|key,value| yield key}
      (@top_index_block||[]).each {|key,ir| yield key unless @deleted_records[key]}
    end

    #***************************************************
    # IndexBlock interface compatibility
    #***************************************************
    # provided for compatibility so ChunkFile object can be the "parent" of an IndexBlock
    def chunk; self; end
    def index_depth; -1; end

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
      @top_index_block = IndexBlock.new(self,"",0,@index_level_lengths[0],:io_stream=>io_stream)
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

    #*************************************************************
    # additional useful internal API
    #*************************************************************
    def fetch_record(key)
      (@records[key] || locate_index_record(key)) unless @deleted_records[key]
    end

    def locate_index_record(key)
      @top_index_block && @top_index_block.locate(key)
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
      (@records[key] || (!@deleted_records[key] && locate_index_record(key))) && true
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

    # this is the baseline algorithm
    # It scans N/2 records, all off of disk, unless they are already cached. Note, that it isn't as bad as it could be - it only reads index records, it doesn't read actual record data
    # returns array: [middle_key, sizes < middle_key, sizes >= middle_key]
    # Guarantees:
    # if records.length > 0 then size1 is > 0
    # if records.length > 1 then size2 is also > 0
    # size1 + size2 == accounting_size
    def middle_key_and_sizes_slow
      chunk_accounting_size=accounting_size
      half_size=chunk_accounting_size/2
      mem_record_keys = @records.keys.sort
      mrk_index = 0

      accounting_offset=0
      (@top_index_block||[]).each do |record|
        while (key=mem_record_keys[mrk_index]) && key < record.key
          asize=@records[key].accounting_size
          return [key,accounting_offset,chunk_accounting_size-accounting_offset] if accounting_offset + asize/2 > half_size
          accounting_offset+=asize
          mrk_index+=1
        end
        next if @deleted_records[record.key]  # skip deleted records
        return [record.key,accounting_offset,chunk_accounting_size-accounting_offset] if accounting_offset + record.accounting_size/2 > half_size
      end
      while key=mem_record_keys[mrk_index]
        asize=@records[key].accounting_size
        return [key,accounting_offset,chunk_accounting_size-accounting_offset] if accounting_offset + asize/2 > half_size
        accounting_offset+=asize
        mrk_index+=1
      end
      raise "this should never happen"
    end
    alias :middle_key_and_sizes :middle_key_and_sizes_slow
  end
end
