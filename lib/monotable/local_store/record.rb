module Monotable

  class Record
    attr_accessor :accounting_size
    attr_accessor :key
    attr_accessor :disk_offset
    attr_accessor :disk_length
    attr_accessor :chunk

    def initialize(chunk=nil)
      @chunk=chunk
      @disk_offset=0
    end

    def inspect
      "<#{self.class} key=#{key.inspect} fields=#{fields.inspect}>"
    end

    # return true if this record no longer contains valid data
    def valid?
      true
    end

    def each
      fields.each {|k,v| yield k,v}
    end
    include Enumerable

    def accounting_size
      @accounting_size||=calculate_accounting_size
    end
    alias :memory_size :accounting_size

    def [](key) fields[key] end
    def []=(key,v) fields[key]=v end

    # to basic ruby data structures
    def to_ruby
      [key,fields]
    end

    def to_json(a=nil,b=nil)
      [key,fields].to_json
    end

    def update(new_fields)
      fields.update(new_fields)
    end

    def length() fields.length end
    def keys() fields.keys end
    def ==(other) other && length==other.length && fields.each {|k,v| return false unless v==other[k]} end

    def Record.parse_record(data,column_dictionary,column_hash=nil)
      fields={}
      di=0
      data = StringIO.new(data) if data.kind_of?(String)
      until data.eof?
        col_id = data.read_asi
        col = column_dictionary[col_id]

        if column_hash && !column_hash[col]
          # we could be more efficient and skip over the col_data if the column isn't in the column_hash
          # currently: READ AND IGNORE
          col_data = data.read_asi_string
        else
          d=data.read_asi_string
          fields[col] = d
        end
      end
      fields
    end

    def calculate_accounting_size(fs=nil)
      sum=key.length #+ MINIMUM_CHUNK_RECORD_OVERHEAD_IN_BYTES
      (fs||fields).each do |k,v|
        sum+=k.length + v.length
      end
      sum
    end

    def encoded(columns)
      fields_local=fields
      keys.collect do |key|
        val=fields_local[key]
        columns[key].to_asi + val.to_asi_string if val  # nil field values are not stored
      end.compact.join
    end

    def parse_index_record(io_stream,last_key)
      prefix_length = io_stream.read_asi
      suffix = io_stream.read_asi_string
      @key = last_key[0,prefix_length] + suffix
      @disk_offset += io_stream.read_asi
      @disk_length = io_stream.read_asi
      @accounting_size = io_stream.read_asi
      self
    end

    def encode_index_record(last_key)
      prefix_length = Tools.longest_common_prefix(key,last_key)
      [
      prefix_length.to_asi,
      (key.length-prefix_length).to_asi,
      key[prefix_length..-1],
      @disk_offset.to_asi,
      @disk_length.to_asi,
      accounting_size.to_asi,
      ].join
    end

    #api for derived classes to implement:
    #def fields(column_hash=nil) end
    #def update(column_hash) end
  end

  class MemoryRecord < Record
    attr_accessor :fields

    def fields(columns_hash=nil)
      if columns_hash
        Tools.select_columns(@fields,columns_hash)
      else
        @fields
      end
    end

    def parse_record(fields,columns_dictionary)
      @fields=Record.parse_record(fields,columns_dictionary)
      self
    end

    def init(key,fields=nil,column_dictionary=nil,columns_hash=nil)
      @key=key
      if fields
        if fields.respond_to?(:eof?) || fields.kind_of?(String)
          @fields=Record.parse_record(fields,column_dictionary,columns_hash)
        else
          fields=Tools.select_columns(fields,columns_hash) if columns_hash
          self.fields=fields
        end
      else
        @fields={}
      end
      self
    end

    def validate_fields(fields)
      raise "fields must be a hash" unless fields.kind_of? Hash
      fields.each do |k,v|
        raise "keys must be strings (k.class == #{k.class})" unless k.kind_of? String
        raise "fields must be strings (v.class == #{v.class})" unless v.kind_of? String
      end
    end

    def fields=(fields)
      validate_fields(fields)
      @fields=fields
      @accounting_size=nil
      fields
    end

    def update(column_hash)
      validate_fields(column_hash)
      fields.merge! column_hash
      recalc_size
      fields
    end
  end

  class DiskRecord < Record
    attr_accessor :file_handle
    attr_accessor :column_dictionary
    attr_accessor :sub_index_block    # only used if this is not an actual record by instead an index -record pointing to a sub-IndexBlock

    def sub_block_key
      @sub_block_key ||= [:index_block, @key]
    end

    def initialize(chunk=nil,data_block_offset=nil)
      @chunk=chunk
      if chunk
        @file_handle=chunk.file_handle
        @column_dictionary=chunk.columns
      end
      @disk_offset=data_block_offset || (chunk && chunk.data_block_offset)
    end

    def inspect
      attrs=[:key,:disk_offset,:disk_length].collect {|k| "#{k}=#{self.send(k).inspect}"}.join(" ")
      "<DiskRecord #{attrs}>"
    end

    def init(key,offset,length,accounting_size,file_handle=nil,column_dictionary=nil)
      @key=key
      @file_handle=file_handle
      @disk_offset=offset
      @disk_length=length
      @accounting_size=accounting_size
      @column_dictionary=column_dictionary
      self
    end

    def [](key)
      fields({key=>true})
    end

    def fields(column_hash=nil)
      data=file_handle.read(disk_offset,disk_length,true)
      Record.parse_record(data,column_dictionary,column_hash)
    end

    def match_to_entry_on_disk(entry_offset_on_disk,entry_file_handle=nil,entry_columns_dictionary=nil)
      @disk_offset+=entry_offset_on_disk
      @file_handle=entry_file_handle if entry_file_handle
      @column_dictionary=entry_columns_dictionary if entry_columns_dictionary
    end
  end

  class JournalDiskRecord < Record
    attr_accessor :journal
    attr_accessor :disk_offset
    attr_accessor :disk_length

    def stringify_values(hash)
      hash.each {|k,v| hash[k]=v.to_s}
    end

    # journal_info require:
    #   :journal => journal object
    #   :offset => offset of entry on disk
    #   :length => length of entry on disk
    def initialize(chunk,key,record,journal_info)
      @chunk = chunk
      @chunk_memory_revision = chunk.memory_revision
      @key = key
      stringify_values(record) if record.kind_of? Hash
      self.journal=journal_info[:journal]
      self.disk_offset=journal_info[:offset]
      self.disk_length=journal_info[:length]
      self.accounting_size=case record
      when Record then record.accounting_size
      when Hash then calculate_accounting_size(record)
      else raise "invalid record class #{record.class}"
      end
    end

    def valid?
      @chunk.memory_revision == @chunk_memory_revision
    end

    def [](key)
      fields({key=>true})
    end

    def fields(columns_hash=nil)
      f=journal.read_entry(disk_offset,disk_length)[:fields]
      f=Tools.select_columns(f,columns_hash) if columns_hash
      f
    end
  end
end
