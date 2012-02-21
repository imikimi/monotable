# encoding: BINARY
=begin

Chunk format:
  Header: "MonotableChunk"
  ASI:            Major version (major changes are not backward compatible)
  ASI:            Minor version (minor changes are backward compatible)
  ASI-String:     Chunk checksum of the EntryBody
  ASI-String:     EntryBody

EntryBody format:
  ASI-String:     Info-Block
  XBD-Dictionary: Column-name Dictionary
  ASI-String:     Index
  (special):      Data-Block

Info-Block:
  XBD:            Info-Block data

Index:
  Index-Pre-Block
  Index-Block(s)

Index-Pre-Block:
  ASI:            N Index-Block-Levels
  N-ASIs:         Byte-size of each index-level

Index-Block:
  Can be any size, but are expected to be close to DEFAULT_MAX_INDEX_BLOCK_SIZE.
  The size of the index-block is known based on the Index-Pre-Block or the index-block's parent index-block.
  Consists of 0 or more index records, in alpha-ascending order

  Index-Block-Record(IBR) Common
    ASI:            # of characters from the previous key to prepend to this Key
                    The first record in a block references the key in the parent Index-Block-Record that pointed to this block.
                    If this is the top-most Index-Block, then this value should always be 0.
    ASI-String:     the rest of the Key
    ASI:            offset of the sub-index-block/record-data in the next-index-level/data-block (0 == first byte of next-index-level or data-block)
    ASI:            length of the sub-index-block/record-data in the next-index-level/data-block

  Notes:  Keys in non-leaf index blocks are the last key of the PREVIOUS
  sub-index-block. In other words, all entries in THIS index-record's sub-block
  are strictly > THIS index-record's key. The very first index-record of each
  level is technically a special case as it's key is "" and it is posible there
  is a user-record with the key "".

Data-Block: consist of 0 or more data-records - as many as fit in the length of the record as specified in the index
  The records are just 0 or more fields. The number of fields per record is known because the index-block-entry for
  the record specifies its byte-size.

Record-Field:
  ASI:            column-ID
  ASI-String:     column-data

=end

module Monotable
  module ChunkMemoryRevisions
    def memory_revision
      @memory_revision||=1
    end

    def next_memory_revision
      @memory_revision=(@memory_revision||0)+1
    end

    def reset
      next_memory_revision
    end
  end

  # see ReadAPI
  module ChunkReadAPI
    include ReadAPI

    # see ReadAPI#get_record
    def get_record(key)
      @records[key]
    end

    # see ReadAPI
    def get_first(options={})
      records=self.records
      options = Tools.normalize_range_options(options)
      gte_key=options[:gte]
      lte_key=options[:lte]
      limit=options[:limit]
      next_options = nil

      res=[]
      each_key do |k|
        break if res.length>=limit || k > lte_key
        res << get_record(k) if k>=gte_key
      end
      if range_end!=:infinity && lte_key >= range_end && res.length < limit
        next_options=options.clone
        next_options[:limit]-=res.length
        next_options[:gte]=range_end
      end
      {:records=>res,:next_options=>next_options}
    end

    # see ReadAPI
    def get_last(options={})
      options = Tools.normalize_range_options(options)
      gte_key=options[:gte]
      lte_key=options[:lte]
      limit=options[:limit]

      res=[]
      reverse_each_key do |k|
        break if res.length>=limit || k < gte_key
        raise "#{self.class}(#{object_id})#get_last(#{options.inspect}) fail! get_record(#{k.inspect})=#{get_record(k).inspect} keys=#{keys.inspect}, get_last chunk-key-range=#{range_start.inspect}..#{range_end.inspect}" unless get_record(k)
        res << get_record(k) if k<=lte_key
      end
      if gte_key < range_start && res.length < limit
        next_options=options.clone
        next_options[:limit]-=res.length
        next_options[:lte]=range_start.binary_prev(DEFAULT_MAX_KEY_LENGTH)
      end
      {:records=>res.reverse,:next_options=>next_options}
    end
  end

  # see WriteAPI
  module ChunkWriteAPI
    include WriteAPI

    # see WriteAPI
    def set(key,fields)
      record=case fields
      when Hash then MemoryRecord.new.init(key,fields)
      when Record then fields
      else raise "value must be a Hash or Record"
      end
      set_internal(key,record)
    end

    # see WriteAPI
    def update(key,columns)
      fields = ((record=get_record(key)) && record.fields) || {}
      fields.update(columns)
      ret=set(key,fields) # call set so DiskChunk can override it
      ret[:result]=:updated if ret[:result]==:replaced
      ret
    end

    # see WriteAPI
    def delete(key)
      delete_internal(key)
    end
  end

  class Chunk
    HEADER_STRING = "MonotableChunk"
    MAJOR_VERSION = 0
    MINOR_VERSION = 0
    HEADER = HEADER_STRING + MAJOR_VERSION.to_asi + MINOR_VERSION.to_asi

    attr_accessor :info
    attr_accessor :columns
    attr_accessor :records
    attr_accessor :accounting_size         # the bytesize of all keys, field-names and field-values

    attr_accessor :range_start  # all keys are >= range_start; nil == first possible key
    attr_accessor :range_end    # all keys are < range_end; nil or :infinity == last possible key

    attr_accessor :data_block_offset
    attr_accessor :file_handle

    attr_accessor :max_chunk_size
    attr_accessor :max_index_block_size

    include ChunkMemoryRevisions
    include ChunkReadAPI
    include ChunkWriteAPI

    # return a json-friendly version of the range_end
    def symbolless_range_end;
      @range_end == :infinity ? nil : @range_end
    end

    def init_chunk(options={})
      @path_store = options[:path_store]
      @max_chunk_size = options[:max_chunk_size] || ((ps=options[:path_store]) && ps.max_chunk_size) || DEFAULT_MAX_CHUNK_SIZE
      @max_index_block_size = options[:max_index_block_size] || ((ps=options[:path_store]) && ps.max_index_block_size) ||  DEFAULT_MAX_INDEX_BLOCK_SIZE

      @range_start=options[:range_start] || ""
      @range_end=options[:range_end] || :infinity
      @records=options[:records] || {}
      @accounting_size=0
      @loaded_record_count=0
    end

    def local_store
      @local_store||=@path_store && @path_store.local_store
    end

    def range
      [range_start,range_end]
    end

    def info; @info||=Xbd::Tag.new("info") end

    def []=(key,value) set(key,value); end

    # options:
    #   :records => {}
    #   :data => string or io_stream
    #   :file_handle
    def initialize(options={})
      init_chunk(options)
      parse(options[:data]) if options[:data]
    end

    def data_loaded?; @data_loaded; end

    # returns a list of all keys in the chunk (unsorted)
    def keys; @records.keys; end

    # returns number of records in the chunk
    def length; @records.length; end

    def inspect
      "<#{self.class} range_start=#{range_start.inspect} range_end=#{range_end.inspect} accounting_size=#{accounting_size} length=#{length}>"
    end

    #***************************************************
    # Iterators
    #***************************************************
    # yields each key in the chunk in sorted order
    def each_key
      keys.sort.each {|key| yield key}
    end

    # yields each key in the chunk in reverse sorted order
    def reverse_each_key
      keys.sort.reverse_each {|key| yield key}
    end

    # yields each key in the chunk, not necessarilly in sorted order
    # this is (potentially) faster than each_key
    def each_key_unsorted
      @records.each {|k,v| yield k}
    end

    # yields every record in the chunk in sorted Key-order
    def each_record
      keys.sort.each {|key| yield @records[key]}
    end

    #**********************************************************************
    # Multiple-MemoryChunk tools
    #**********************************************************************
    #sort chunks based on their range_start
    def <=>(other) range_start <=> other.range_start end

    # returns true if the key is within the range covered by this chunk
    # if either range_start or range_end is not set, it is a while-card - always matches
    def in_range?(key)
      (key>=range_start) && (range_end==:infinity || key < range_end)
    end

    def to_s
      range_start
    end

    #**********************************************************************
    # saved_chunk_info
    #**********************************************************************

    def saved_chunk_info
      @saved_chunk_info||= begin
        info.tag("chunk") || (info<<Xbd::Tag.new("chunk"))
      end
    end

    def load_saved_chunk_info
      sci=saved_chunk_info
      @range_start = sci["range_start"] || ""
      @range_end = sci["range_end"] || :infinity
      @accounting_size = (sci["accounting_size"] || 0).to_i
      @loaded_record_count = (sci["record_count"] || 0).to_i
      @max_chunk_size = (sci["max_chunk_size"] || DEFAULT_MAX_CHUNK_SIZE).to_i
      @max_index_block_size = (sci["max_index_block_size"] || DEFAULT_MAX_INDEX_BLOCK_SIZE).to_i
    end

    def save_saved_chunk_info
      sci=saved_chunk_info
      sci["range_start"] = @range_start
      sci["range_end"] = symbolless_range_end
      sci["accounting_size"] = @accounting_size
      sci["record_count"] = length
      sci["max_chunk_size"] = @max_chunk_size
      sci["max_index_block_size"] = @max_index_block_size
    end

    #################################
    # maintain @accounting_size
    #################################
    # (self.record(key) || record) must exist
    # returns the new size delta
    def add_size(key,record=nil)
      delta=(record || get_record(key)).accounting_size
      @accounting_size+=delta
      delta
    end

    # returns the new size delta
    def sub_size(key,record=nil)
      record||=get_record(key)
      delta=(record && record.accounting_size) || 0
      @accounting_size-=delta
      delta
    end

    def calculate_accounting_size
      sum=0
      records.each do |k,v|
        sum += v.accounting_size
      end
      sum
    end

    def update_accounting_size
      self.accounting_size=calculate_accounting_size
    end

    #################################
    # verification
    ################################
    def verify_accounting_size
      actual_size=calculate_accounting_size
      throw "accounting_size(#{accounting_size.inspect}) does not match the actual_size(#{actual_size.inspect})" unless actual_size==accounting_size
    end

    def verify_records
      records.each do |key,record|
        raise "invalid record class: #{record.class} (#{record.inspect})" unless record.kind_of?(Record)
      end
    end

    #################################
    # bulk edits
    #################################
    # returns {:result => :replaced} or {:result => :created}
    def set_internal(key,record)
      sub_delta = sub_size(key)
      @records[key]=record
      add_delta = add_size(key,record)
      {:result=> sub_delta!=0 ? :replaced : :created, :size_delta=>add_delta-sub_delta, :size=>record.accounting_size}
    end

    def delete_internal(key)
      sub_delta = sub_size(key)
      records.delete(key)
      {:result=> sub_delta!=0 ? :deleted : :noop, :size_delta=>-sub_delta}
    end

    def bulk_set(records)     records.each {|k,v| set_internal(k,v)} end
    def bulk_update(records)  records.each {|k,v| update(k,v)} end
    def bulk_delete(keys)     keys.each {|key| delete_internal(key)} end

    #***************************************************
    # MemoryChunk Splitting
    #***************************************************
    # all keys >= on_key are put into a new entry
    def split_into(on_key,second_chunk)
      # TODO: if @records where an RBTree, couldn't we just do a spit in O(1) ?
      @records.keys.each do |key|
        second_chunk.records[key] = @records.delete(key) if key >= on_key
      end

      # update ranges of both chunks
      second_chunk.range_end = @range_end
      second_chunk.range_start = @range_end = on_key

      # return second_chunk
      second_chunk
    end

    # all keys >= on_key are put into a new chunk
    def split(on_key=nil,to_filename=nil)
      if on_key
        size1,size2=split_on_key_sizes(on_key)
      else
        on_key,size1,size2=middle_key_and_sizes
      end
      to_filename||=path_store.generate_filename

      # create new chunk
      second_chunk_file=DiskChunk.init(:filename=>to_filename,:journal=>journal,:max_chunk_size=>max_chunk_size,:path_store => path_store)

      # do the actual split
      # NOTE: this just splits the in-memory Records. If they are DiskRecords, they will still point to the same file, which is correct for reading.
      split_into(on_key,second_chunk_file)

      # update the path_store (which will also update the local_store)
      local_store.add_chunk(second_chunk_file) if local_store

      # set entry
      journal.split(file_handle,on_key,to_filename)

      # update sizes
      self.accounting_size=size1 || self.calculate_accounting_size
      second_chunk_file.accounting_size=size2 || second_chunk_file.calculate_accounting_size

      # return the new DiskChunk object
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

    # Find the middle-most key of the chunk
    #
    # Baseline (slow) algorithm
    #   It scans N/2 records, potenitally all off of disk, unless they are already cached.
    #   Note, that it isn't as bad as it could be - it only reads index records, it doesn't need to read actual record data
    #
    # returns array: [middle_key, size1, size2]
    #   size1 = sum of all accounting_sizes for records with keys < middle_key
    #   size2 = sum of all accounting_sizes for records with keys >= middle_key
    #
    # Guarantees:
    #   if records.length > 0 then size1 is > 0
    #   if records.length > 1 then size2 is also > 0
    #   size1 + size2 == accounting_size
    def middle_key_and_sizes_slow
      chunk_accounting_size = accounting_size
      half_size  = chunk_accounting_size/2
      accounting_offset = 0

      each_record do |record|
        asize = record.accounting_size
        return [record.key, accounting_offset, chunk_accounting_size-accounting_offset] if accounting_offset + asize/2 > half_size
        accounting_offset += asize
      end
      raise "this should never happen"
    end
    alias :middle_key_and_sizes :middle_key_and_sizes_slow

    #***************************************************
    # Parsing Helpers
    #***************************************************
    # actual parsing is done by the "parse(io_stream)" method, which each MemoryChunk class implements based on
    # what parts of the chunk it wants to parse.

    def parse_header(io_stream)
      test_header_string=io_stream.read(HEADER_STRING.length)
      raise "invalid MemoryChunk: #{test_header_string.inspect}!=#{HEADER_STRING.inspect}" unless test_header_string==HEADER_STRING
      major_version = io_stream.read_asi
      minor_version = io_stream.read_asi
      raise "unsupported format version #{major_version}" unless major_version<=MAJOR_VERSION
    end

    def parse_info_block(io_stream)
      self.info = Xbd.parse io_stream.read_asi_string
      load_saved_chunk_info
    end

    def parse_columns_block(io_stream)
      columns_block = io_stream.read_asi_string
      @columns,index = Xbd::Dictionary.parse(columns_block.length.to_asi + columns_block,0)
    end

    def parse_index_block(io_stream,record_type)
      # new multi-level index reading
      num_index_levels=io_stream.read_asi
      index_level_lengths=[]
      num_index_levels.times {index_level_lengths<<io_stream.read_asi}

      # hack to skip the higher index levels
      leaves_length=index_level_lengths.pop
      index_level_lengths.each {|length| io_stream.read(length)}

      # read the lower-most index entirely into memory
      last_key=""
      end_pos=io_stream.pos + leaves_length
      records={}
      while io_stream.pos < end_pos #io_stream.eof?
        record=record_type.new(self).parse_index_record(io_stream,last_key)
        last_key=record.key
        records[record.key]=record
      end
      records
    end

    #***************************************************
    # Encoding
    #***************************************************
    # disk_records logs the offset and index of every record in the entry-binary-string returned
    # it seems, with some quick benchmarking, that we could speed up writing a chunk to disk by at least 2x if we just:
    #   write to disk as we go AND
    #   decrese the amount of string-copying
    def to_binary(return_disk_records=nil)
      disk_records = return_disk_records || {}
      # encode info-block
      save_saved_chunk_info
      info_block_string = info.to_binary.to_asi_string

      # encode column-dictionary
      self.columns=Xbd::Dictionary.new
      @records.each {|key,col_data| col_data.keys.each {|col| columns << col}}
      column_dictionary_string = columns.to_binary

      #pre-sort keys so we only have to sort them once
      sorted_keys=keys.sort

      # encode data-block
      # populates disk_records
      data_block_string = encoded_data_block(disk_records, sorted_keys)

      # encode index-block
      # uses disk_records
      index_block_string = encoded_index_block(disk_records, sorted_keys)

      # encode entry body
      entry_body = [info_block_string, column_dictionary_string, index_block_string, data_block_string].join

      # checksum
      checksum_prefix = Tools.asi_checksum_string_prefix(entry_body)

      # += all offsets by data_block_offset
      if return_disk_records
        data_block_offset = HEADER.length + checksum_prefix.length + info_block_string.length+ column_dictionary_string.length+ index_block_string.length
        disk_records.each {|k,v| v.match_to_entry_on_disk(data_block_offset)}
      end

      # encode entry
      [HEADER, checksum_prefix, entry_body].join
    end

    def save(filename)
      filename += CHUNK_EXT unless filename[-CHUNK_EXT.length..-1]==CHUNK_EXT
      File.open(filename,"wb") {|f| f.write to_binary}
      filename
    end

    private

    # Encode the index block
    # Must encode the data-block first to generate disk_records:
    #   disk_records is a hash keyed by the record keys. The values are arrrays of: [offset, length] for each encoded record in the data-block.
    # Other params:
    #   sorted_keys - an optimization measure, can safely be left empty, but if you already have a sorted list of the keys, provided it for a speed improvement
    #   if log_index is set, it should be an empty Hash. A DiskRecord of every record saved is added to the log_index.
    def encoded_index_block(disk_records, sorted_keys=nil)
      sorted_keys||=@records.keys.sort
      IndexBlockEncoder.encode(:max_index_block_size => max_index_block_size) do |ibe|
        index_block_string = sorted_keys.collect do |key|
          dr=disk_records[key]
          ibe.add(key,dr.disk_offset,dr.disk_length,dr.accounting_size)
        end
      end
    end

    # A DiskRecord of every record saved is added to the disk_records hash at the associated key for that record
    def encoded_data_block(disk_records={},sorted_keys=nil)
      sorted_keys||=@records.keys.sort
      offset=0
      encoded_data = sorted_keys.collect do |key|
        record=@records[key]
        encoded_record = record.encoded(columns)
        disk_records[key] = DiskRecord.new(self).init(key,offset,encoded_record.length,record.accounting_size)  # offset and length of encoded data
        offset += encoded_record.length
        encoded_record
      end.join
      encoded_data
    end

  end

end
