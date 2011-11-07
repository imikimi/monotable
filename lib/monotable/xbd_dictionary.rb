
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

    def Dictionary.sanitize_string(str)
      case str
      when String then  "#{str}".force_encoding("BINARY")
      else              str.to_s.force_encoding("BINARY")
      end
    end

    # add a String to the dictionary
    def <<(str)
      str = Dictionary.sanitize_string str
      @hash[str] ||= begin
        new_id = @array.length
        @array << @hash[new_id] = str
        new_id
      end
    end

    # convert to binary string
    def to_binary
      encoded_dictionary = [@array.length.to_asi, @array.collect{|v| v.length.to_asi}, @array].join
      encoded_dictionary.to_asi_string
    end

    class <<self
      def read_encoded_dictionary(source,index)
        encoded_dictionary, index = source.read_asi_string index
        [StringIO.new(encoded_dictionary), index]
      end

      def read_num_entries(encoded_dictionary)
        encoded_dictionary.read_asi
      end

      def read_string_lengths(encoded_dictionary,num_entries)
        num_entries.times.collect {encoded_dictionary.read_asi}
      end

      def read_strings(encoded_dictionary,lengths)
        lengths.collect {|len| encoded_dictionary.read len}
      end

      # parses dictionary data from a "source" string at offset "index"
      # returns the parsed Dictionary object and the first "index" after the data read
      def parse(source,index=0)
        encoded_dictionary,index = read_encoded_dictionary(source,index)
        num_entries = read_num_entries encoded_dictionary
        lengths = read_string_lengths encoded_dictionary, num_entries
        strings = read_strings encoded_dictionary, lengths
        [Dictionary.new(strings), index]
      end
    end
  end
end
