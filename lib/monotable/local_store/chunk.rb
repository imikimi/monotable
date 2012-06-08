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

    def reset(updated_location=nil)
      @saved_chunk_info=nil
      if updated_location
        @file_handle && @file_handle.close
        @file_handle = FileHandle.new updated_location
      elsif file_handle
        @file_handle.close
      else
        #puts "#{self.class}#reset huh?"
      end
      @records={}
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
      normalized_options = Tools.normalize_range_options(options)
      #puts "#{self.class}#get_first(#{options.inspect}) normalized_options=#{normalized_options.inspect}"
      gte_key=normalized_options[:gte]
      lte_key=normalized_options[:lte]
      limit = normalized_options[:limit]
      next_options = nil

      res=[]
      each_key do |k|
        #puts "#{self.class}#get_first() object_id=#{self.object_id} k=#{k.inspect}"
        break if res.length>=limit || k > lte_key
        res << get_record(k) if k>=gte_key
      end
      if lte_key >= range_end && res.length < limit
        next_options=options.clone
        next_options[:limit]=limit - res.length
        next_options[:gte]=range_end
        next_options.delete(:gt)
      end
      {:records=>res,:next_options=>next_options}
    end

    # see ReadAPI
    def get_last(options={})
      normalized_options = Tools.normalize_range_options(options)
      #puts "#{self.class}#get_last(#{options.inspect}) normalized_options=#{normalized_options.inspect}"
      gte_key=normalized_options[:gte]
      lte_key=normalized_options[:lte]
      limit=normalized_options[:limit]

      res=[]
      reverse_each_key do |k|
        break if res.length>=limit || k < gte_key
        raise "#{self.class}(#{object_id})#get_last(#{options.inspect}) fail! get_record(#{k.inspect})=#{get_record(k).inspect} keys=#{keys.inspect}, get_last chunk-key-range=#{range_start.inspect}..#{range_end.inspect}" unless get_record(k)
        res << get_record(k) if k<=lte_key
      end
      if gte_key < range_start && res.length < limit
        next_options=options.clone
        next_options[:limit]=limit - res.length
        next_options[:lt]=range_start
        next_options.delete(:lte)
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
      ret[:result]="updated" if ret[:result]=="replaced"
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

    attr_accessor :range        # all keys are within this range. This is a ruby, right-open-ended range: range_start_key...range_end_key

    attr_accessor :data_block_offset
    attr_accessor :file_handle
    attr_accessor :basename

    attr_accessor :max_chunk_size
    attr_accessor :max_index_block_size

    # compact_to_store_path is used temporarilly during compaction to note the root-path
    # of the store the compacted chunk will be written to
    attr_accessor :compact_to_store_path
    attr_accessor :record_count_on_disk

    include ChunkMemoryRevisions
    include ChunkReadAPI
    include ChunkWriteAPI

    def init_chunk(options={})
      #puts "#{self.class}#init_chunk(#{options.inspect})"
      # full_path
      # file_handle
      # filename
      @file_handle = FileHandle.new(options[:filename]) if options[:filename]
      @basename = options[:basename] || (filename && File.basename(filename))

      @compact_to_store_path = options[:compact_to_store_path]
      @path_store = options[:path_store]
      @max_chunk_size = options[:max_chunk_size] || ((ps=options[:path_store]) && ps.max_chunk_size) || DEFAULT_MAX_CHUNK_SIZE
      @max_index_block_size = options[:max_index_block_size] || ((ps=options[:path_store]) && ps.max_index_block_size) ||  DEFAULT_MAX_INDEX_BLOCK_SIZE

      @range = (options[:range_start] || FIRST_POSSIBLE_KEY) ... (options[:range_end] || LAST_POSSIBLE_KEY)
      @records = options[:records] || {}
      @accounting_size =0
      @record_count_on_disk = 0
    end

    def valid_range?
      range_start < range_end
    end

    def valid?
      valid_range?
    end

    # Does this chunk's range cover the provided key?
    # i.e. If they key exists, would it be contained in this chunk?
    def cover?(key)
      range.cover? key
    end

    def Chunk.cover?(range,key)
      key >= range.first && key < range.last
    end

    def path_store_changed?
      raise "#{self.class}#path_store_chaned? path_store is nil" unless path_store
      #puts "path_store_changed? path_store.path = #{path_store.path} filename = #{filename}"
      !path_store.contains_file? filename
    end

    # note that this range is inclusive left, exclusive right: [a..b)
    def range_start;  range.first; end
    def range_end;    range.last; end

    def range_start=(s);    @range = s...range_end; end
    def range_end=(e);      @range = range_start...e; end

    def filename
      @file_handle && @file_handle.to_s
    end

    # fh can be a FileHandle or the filename (as a string)
    def file_handle=(fh)
      @file_handle = fh.kind_of?(FileHandle) ? fh : FileHandle.new(fh)
    end

    # the key for the index record for this chunk
    def index_key
      GlobalIndex.index_key_for_chunk(self)
    end

    def local_store
      @local_store||=@path_store && @path_store.local_store
    end

    def status
      {
      :range_start => range_start,
      :range_end => range_end,
      :accounting_size => accounting_size,
      :record_count => length
      }
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
      (key>=range_start) && (range_end==LAST_POSSIBLE_KEY || key < range_end)
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
      @range = (sci["range_start"] || FIRST_POSSIBLE_KEY) ... (sci["range_end"] || LAST_POSSIBLE_KEY)
      @accounting_size = (sci["accounting_size"] || 0).to_i
      @record_count_on_disk = (sci["record_count"] || 0).to_i
      @max_chunk_size = (sci["max_chunk_size"] || DEFAULT_MAX_CHUNK_SIZE).to_i
      @max_index_block_size = (sci["max_index_block_size"] || DEFAULT_MAX_INDEX_BLOCK_SIZE).to_i
      #puts "#{self.class}#load_saved_chunk_info sci=#{sci.inspect}"
    end

    def save_saved_chunk_info
      #puts "#{self.class}#save_saved_chunk_info length=#{length}"
      sci=saved_chunk_info
      sci["range_start"] = range_start
      sci["range_end"] = range_end
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
    # returns {:result => "replaced"} or {:result => "created"}
    def set_internal(key,record)
      sub_delta = sub_size(key)
      @records[key]=record
      add_delta = add_size(key,record)
      {:result=> sub_delta!=0 ? "replaced" : "created", :size_delta=>add_delta-sub_delta, :size=>record.accounting_size}
    end

    def delete_internal(key)
      sub_delta = sub_size(key)
      records.delete(key)
      {:result=> sub_delta!=0 ? "deleted" : "no-op", :size_delta=>-sub_delta}
    end

    def bulk_set(records)     records.each {|k,v| set_internal(k,v)} end
    def bulk_update(records)  records.each {|k,v| update(k,v)} end
    def bulk_delete(keys)     keys.each {|key| delete_internal(key)} end

    #***************************************************
    # MemoryChunk Splitting
    #***************************************************

    # Alter SELF and create a new object of the same type, updating all data-structures to
    # represent the split. SELF is everything < on_key. new_chunk is everything >= on_key
    # Required Options
    #   :new_chunk_accounting_size => the accounting_size for the new chunk
    #   :new_chunk_record_count => number of records in the new chunk
    #   :on_key
    # Optional Options
    #   :new_chunk => a new chunk object or nil, in which case a new object will be created
    #   :to_basename => override basename in new_chunk
    def split_simple(options={})
      Tools.required options, :on_key, :new_chunk_accounting_size, :new_chunk_record_count, :old_chunk_accounting_size, :old_chunk_record_count

      (options[:new_chunk]||=self.clone).tap do |new_chunk|

        on_key = options[:on_key]
        # update range
        new_chunk.range_end   = self.range_end
        new_chunk.range_start = self.range_end = on_key

        # split records
        new_chunk.records = records.select {|key,value| key >= on_key}
        self.records      = records.select {|key,value| key < on_key}

        # update accounting size
        new_chunk.accounting_size = options[:new_chunk_accounting_size]
        self.accounting_size = options[:old_chunk_accounting_size]

        # update misc
        new_chunk.basename = options[:to_basename] if options[:to_basename]
      end
    end

    # all keys >= on_key are put into a new chunk
    # returns the new chunk
    # Options can be the on_key string, or a hash:
    # Options-Hash:
    #   :on_key
    #   :to_basename
    def split(options={})
      options = {on_key:options} if options.kind_of? String
      options = split_setup options
      split_simple(options)
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
    #
    # Options:
    #   :on_key
    def split_setup_slow(options={})
      on_key = options[:on_key]
      chunk_accounting_size = accounting_size
      half_size  = chunk_accounting_size/2

      accounting_offset = 0
      count = 0
      key = nil
      each_record do |record|
        key = record.key
        asize = record.accounting_size
        break if on_key && (key >= on_key)
        break if !on_key && (accounting_offset + asize/2 > half_size)
        count += 1
        accounting_offset += asize
      end
      options.merge(
        :on_key => on_key || key,
        :new_chunk_accounting_size => accounting_size - accounting_offset,
        :new_chunk_record_count => length - count,
        :old_chunk_accounting_size => accounting_offset,
        :old_chunk_record_count => count
      )
    end
    alias :split_setup :split_setup_slow

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

    # saves to the specified filename or self.filename if non provided
    def save(filename=nil)
      filename ||= self.filename
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
