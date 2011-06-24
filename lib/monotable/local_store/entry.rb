# encoding: BINARY
=begin

Chunks consiste of one or more Entries, append to each other. The first entry is considered the Master. Every subsequent
Entry is a journalized edit to the master entry. These other entries will each have an "action" specified for the kind of edit
they represent.

Entry format:
  Header: "MonoTableChunk"
  ASI:            Major version (major changes are not backward compatible)
  ASI:            Minor version (minor changes are backward compatible)
  ASI-String:     Entry checksum of the EntryBody
  ASI-String:     EntryBody

EntryBody format:
  ASI-String:     Info-Block
  XBD-Dictionary: Column-name Dictionary
  ASI-String:     Index-Block
  (special):      Data-Block

Info-Block
  XBD:            Info-Block data


**********TODO - implement this new Index-Block-Format
It is optimized such that we don't need to load the entire index in memory and yet can do random reads reasonably efficiently.
We are expecting to need to have to manage some 130,000 chunks. The in-memory object-size for each chunk needs to be reasonable and constant.
This should achieve that.

Index-Pre-Block format:
  ASI:            N Index-Block-Levels
  N-ASIs:         Byte-size of each index-level

  Index-Blocks:
    Can be any size, but are expected to be close to by <= some max block-size which is targeted to be 64k.
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
  ASI:            column-ID
  ASI-String:     column-data

=end

module MonoTable

  class Entry
    HEADER_STRING = "MonoTableChunk"
    MAJOR_VERSION = 0
    MINOR_VERSION = 0
    HEADER = HEADER_STRING + MAJOR_VERSION.to_asi + MINOR_VERSION.to_asi

    attr_accessor :info
    attr_accessor :columns
    attr_accessor :records
    attr_accessor :accounting_size         # the bytesize of all keys, field-names and field-values

    attr_accessor :range_start  # all keys are >= range_start; nil == first possible key
    attr_accessor :range_end    # all keys are < range_end; nil == last possible key

    def init_entry(records={})
      @range_start=""
      @range_end=:infinity
      @records=records
      @accounting_size=0
    end

    def range
      [range_start,range_end]
    end


    def info; @info||=Xbd::Tag.new("info") end

    def [](key) @records[key]; end
    def []=(key,value) set(key,value); end

    def initialize(records_or_parse={},file_handle=nil)
      case records_or_parse
      when Hash then  init_entry(records_or_parse)
      else            parse(records_or_parse,file_handle)
      end
    end

    def data_loaded?; @data_loaded; end
    def keys; @records.keys; end
    def length; @records.length; end

    #**********************************************************************
    # Multiple-Chunk tools
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
    end

    def save_saved_chunk_info
      sci=saved_chunk_info
      sci["range_start"] = @range_start
      sci["range_end"] = @range_end == :infinity ? nil : @range_end
      sci["accounting_size"] = @accounting_size
    end

    #*************************************************************
    # Read API
    #*************************************************************
    def get(key,columns=nil)
      if columns
        Tools.select_columns(records[key],columns)
      else
        records[key]
      end
    end

    #*************************************************************
    # Write API
    #*************************************************************
    # value must be a hash or a MonoTable::Record
    def set(key,fields)
      record=case fields
      when Hash then MemoryRecord.new(key,fields)
      when Record then fields
      else raise "value must be a Hash or Record"
      end
      set_internal(key,record)
    end

    def update(key,columns)
      fields = get(key) || {}
      fields.update(columns)
      set(key,fields) # call set so ChunkFile can override it
      fields
    end

    def delete(key)
      delete_internal(key)
    end

    #################################
    # maintain @accounting_size
    #################################
    def add_size(key,record=nil)
      record||=@records[key]
      amount = record.accounting_size
      @accounting_size+=amount
    end

    def sub_size(key,record=nil)
      record||=@records[key]
      return unless record
      amount = record.accounting_size
      @accounting_size-=amount
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
    def set_internal(key,record)
      sub_size(key)
      @records[key]=record
      add_size(key)
      record
    end

    def delete_internal(key)
      sub_size(key)
      records.delete(key)
    end

    def bulk_set(records)     records.each {|k,v| set_internal(k,v)} end
    def bulk_update(records)  records.each {|k,v| update(k,v)} end
    def bulk_delete(keys)     keys.each {|key| delete_internal(key)} end

    # all keys >= on_key are put into a new entry
    def split_into(on_key,second_entry)
      # TODO: if @records where an RBTree, couldn't we just do a spit in O(1) ?
      @records.keys.each do |key|
        second_entry.records[key] = @records.delete(key) if key >= on_key
      end

      # update ranges of both chunks
      second_entry.range_end = @range_end
      second_entry.range_start = @range_end = on_key

      # return second_entry
      second_entry
    end

    def apply_entry(entry)
      # don't apply entries not meant for this chunk
      return if entry.range_start && !in_range?(entry.range_start)

      # init info if not already set
      @info||=entry.info

      # decode action_type
      action_type = (action_tag=entry.info.tag("action")) && action_tag["type"]
      case action_type
      when nil,"set" then bulk_set(entry.records) # set is the default type
      when "update" then bulk_update(entry.records)
      when "delete" then bulk_delete(entry.records.keys)
      when "split" then
        on_key=action_tag["on_key"]
        to_file=action_tag["to_file"]
        second_half=Entry.new(split_entry(on_key))
        second_half.save(to_file)
      when "merge" then
        from_file=action_tag["from_file"]
        source_chunk=self.class.load(from_file)
        bulk_set(source_chunk.records)
      else
        raise "unsupported action type"
      end
    end

    #***************************************************
    # LOADING/SAVING
    #***************************************************

    def parse_index_block(index_block,file_handle,data_block,data_block_offset)
      last_key=""
      i=0

      # new multi-level index reading
      num_index_levels=index_block.read_asi
      index_level_lengths=[]
      num_index_levels.times {index_level_lengths<<index_block.read_asi}

      # hack to skip the higher index levels
      index_level_lengths.pop
      index_level_lengths.each {|length| index_block.read(length)}

      # read the lower-most index entirely into memory
      until index_block.eof?
        prefix_length = index_block.read_asi
        suffix = index_block.read_asi_string
        key = last_key[0,prefix_length] + suffix
        last_key = key
        data_offset = index_block.read_asi
        data_length = index_block.read_asi
        data_accounting_size = index_block.read_asi

        @records[key] = if file_handle
          DiskRecord.new(key,data_offset+data_block_offset, data_length, data_accounting_size, file_handle, @columns)
        else
          MemoryRecord.new(key,data_block[data_offset,data_length],@columns)
        end
      end
    end

    # io_stream can be a String or anything that supports the IO interface
    def parse(io_stream,file_handle=nil)
      # convert String to StringIO
      io_stream = StringIO.new(io_stream) if io_stream.kind_of?(String)

      # parse header
      test_header_string=io_stream.read(HEADER_STRING.length)
      raise "invalid Chunk: #{test_header_string.inspect}!=#{HEADER_STRING.inspect}" unless test_header_string==HEADER_STRING
      major_version = io_stream.read_asi
      minor_version = io_stream.read_asi
      raise "unsupported format version #{major_version}" unless major_version<=MAJOR_VERSION

      @records={}
      if @data_loaded=!file_handle
        # load the entire entry and validate the checksum
        entry_body,ignored_index = Tools.read_asi_checksum_string(io_stream)
        next_entry = io_stream.pos

        # resume parsing from the now-loaded entry_body
        io_stream = StringIO.new(entry_body)
      else
        # ignore the checksum
        checksum = io_stream.read_asi_string

        # read in the rest of the entry, checksummed
        entry_length = io_stream.read_asi
        next_entry = io_stream.pos + entry_length
      end

      # load the info-block
      self.info = Xbd.parse io_stream.read_asi_string
      load_saved_chunk_info

      # load the columns-block
      columns_block = io_stream.read_asi_string
      @columns,index = Xbd::Dictionary.parse(columns_block.length.to_asi + columns_block,0)

      # load the index-block
      index_block = StringIO.new io_stream.read_asi_string

      # data_block
      if file_handle
        data_block_offset = io_stream.pos
      else
        data_block = io_stream.read
      end

      # parse the index-block, and optionally, load the data
      parse_index_block(index_block,file_handle,data_block,data_block_offset)

      #seek to next entry
      io_stream.seek(next_entry)
    end

    # disk_records logs the offset and index of every record in the entry-binary-string returned
    def to_binary(return_disk_records=nil)
      disk_records = return_disk_records || {}
      # encode info-block
      save_saved_chunk_info
      info_block_string = info.to_xbd.to_asi_string

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
      ibe=IndexBlockEncoder.new
      index_block_string = sorted_keys.collect do |key|
        dr=disk_records[key]
        ibe.add(key,dr.offset,dr.length,dr.accounting_size)
      end
      ibe.to_s
    end

    # A DiskRecord of every record saved is added to the disk_records hash at the associated key for that record
    def encoded_data_block(disk_records={},sorted_keys=nil)
      sorted_keys||=@records.keys.sort
      offset=0
      encoded_data = sorted_keys.collect do |key|
        record=@records[key]
        encoded_record = record.encoded(columns)
        disk_records[key] = DiskRecord.new(key,offset,encoded_record.length,record.accounting_size)  # offset and length of encoded data
        offset += encoded_record.length
        encoded_record
      end.join
      encoded_data
    end

  end

  class IndexBlockEncoder
    attr_accessor :last_key
    attr_accessor :index_records
    attr_accessor :current_block_key
    attr_accessor :current_block_offset
    attr_accessor :current_block_length
    attr_accessor :max_index_block_size
    attr_accessor :parent_index_block_encoder
    attr_accessor :total_accounting_size

    def initialize(max_index_block_size=(64*1024))
      @current_block_key=@last_key=""
      @current_block_offset=0
      @index_records=[]
      @current_block_length=0
      @max_index_block_size=max_index_block_size
      @total_accounting_size=0
    end

    def auto_parent_index_block_encoder
      @parent_index_block_encoder ||= IndexBlockEncoder.new(max_index_block_size)
    end

    def to_s
      # TODO: join-in the parent_index_block_encoder and the entier index's pre-block, as described a the top of this file
      # then we need to ensure the decoder can read this new format. The nice thing is the bottom-most index-level is identical to the current
      # format. So we can cheat for the first version - just skip to the bottom-most index-level

      all_index_levels=[]
      ibe=self
      while(ibe)
        all_index_levels<<@index_records.join
        ibe=ibe.parent_index_block_encoder
      end
      all_index_levels.reverse!
      [
      all_index_levels.length.to_asi,
      all_index_levels.collect {|ilevel| ilevel.length.to_asi},
      all_index_levels
      ].flatten.join.to_asi_string
    end

    def add(key,offset,length,accounting_size)
      prefix_length = Tools.longest_common_prefix(key,@last_key)

      # encode record
      encoded_index_record = [
        prefix_length.to_asi,
        (key.length-prefix_length).to_asi,
        key[prefix_length..-1],
        offset.to_asi,
        length.to_asi,
        accounting_size.to_asi,
        ].join

      if max_index_block_size && (current_block_length + encoded_index_record.length > max_index_block_size)
        auto_parent_index_block_encoder.add(current_block_key,current_block_offset,current_block_length,@total_accounting_size)

        @current_block_key=@last_key
        @current_block_offset+=@current_block_length
        @current_block_length=0
        @total_accounting_size=0
      end

      @total_accounting_size+=accounting_size
      @current_block_length += encoded_index_record.length

      @index_records << encoded_index_record
      @last_key=key
    end
  end
end
