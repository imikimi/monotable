
module Xbd
  #*********************************
  # Xbd::Dictionary
  #*********************************
  # Consists of:
  #   @hash:  a map from values to IDs and IDs to values
  #   @array: a list of values; their indexes == their IDs
  #
  class Dictionary
    attr_reader :hash,:array

    def initialize(initial_values=[])
      @hash={}
      @array=[]
      initial_values.each {|v| self<<(v)}
    end

    # return String given an ID, or ID given a String
    def [](i) @hash[i] end

    # add a String to the dictionary
    def <<(name)
      name=case name
      when String then "#{name}".force_encoding("BINARY")
      else name.to_s.force_encoding("BINARY")
      end
      return @hash[name] if @hash[name] # dont add if already exists
      new_id=@array.length
      @array<<name
      @hash[new_id]=name
      @hash[name]=new_id
      name
    end

    # convert to binary string
    def to_binary
      data=@array.length.to_asi + @array.collect {|v| v.length.to_asi}.join + @array.join
      data.length.to_asi + data
    end

    # parses dictionary data from a "source" string at offset "index"
    # returns the parsed Dictionary object and the first "index" after the data read
    def Dictionary.parse(source,index=0)
      start_index=index
      dict_length,index=source.read_asi(index)
      end_dict_index=index+dict_length
      dict_data=source[index..(index+dict_length)-1]
      raise "Invalid Dictionary Data Length (dict_data.length=#{dict_data.length}, dict_length=#{dict_length})" if dict_data.length!=dict_length

      # read num_entries
      num_entries,index=dict_data.read_asi

      # read dictionary string lengths
      lengths=[]
      (0..num_entries-1).each do
        len,index=dict_data.read_asi(index)
        lengths<<len
      end

      # read dictionary strings
      # NOTE: first string has ID=0, second has ID=1, etc...
      data=[]
      lengths.each do |len|
        entry=dict_data[index..(index+len-1)]
        data<<entry
        index+=len
      end

      # return Dictinary and next start index
      return Dictionary.new(data),end_dict_index
    end
  end
end
