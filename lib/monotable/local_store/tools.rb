require "rubygems"
require 'zlib'
#require "inline"
module MonoTable
=begin
  class FastBitField
    attr_reader :size

    def initialize(size, bits=nil)
      @size = size
      @bits = bits || "\000" * (( (size - 1) / 8 ) +1)
    end

    def set(bit) set_c(@bits,bit) end
    def [](bit) get_c(@bits,bit) end
    def to_s; @bits; end

    inline do |compiler|
      compiler.c "void set_c(char *bits, int bit) {bits[bit/8] |= (1 << (bit % 8));}"
      compiler.c "VALUE get_c(char *bits, int bit) {return bits[bit/8] & (1 << (bit % 8)) ? Qtrue : Qfalse;}"
    end
  end
=end
  module Tools

    def Tools.log_error(except,info=nil)
      Log << ["Exception: #{except}",#"Logged from: #{caller()[0]}",
        "Info: #{info}","Trace:","\t#{except.backtrace.join("\n\t")}\n"].join("\n")
    end
    # returns the number of characters s1 and s2 hold in column at the beginning of the strings
    def Tools.longest_common_prefix(s1,s2)
      s1.scan(/./).each_with_index do |c1,i|
        c2=s2[i,1]
        return i unless c1==c2
      end
      s1.length
    end

    def Tools.array_to_hash(a)
      h={}
      a.each {|b| h[b]=true}
      h
    end

    # given an array of columns, select the specified fields
    def Tools.select_columns(record,columns)
      ret={}
      columns.each {|c| ret[c]=record[c]}
      ret
    end

    # if column_hash is set, only return columns that are listed in column_hash, otherwise return all
    def Tools.read_asi_checksum_string(source,index=0)
      checksum,index=source.read_asi_string(index)
      string,index=source.read_asi_string(index)
      test_checksum=Tools.checksum(string)
      raise "checksum failure (#{checksum.inspect}!=#{test_checksum.inspect})" unless checksum==test_checksum
      return string,index
    end

    def Tools.read_asi_checksum_string_from_file(source)
      checksum=source.read_asi_string
      string=source.read_asi_string
      test_checksum=Tools.checksum(string)
      raise "checksum failure (#{checksum.inspect}!=#{test_checksum.inspect})" unless checksum==test_checksum
      return string
    end

    def Tools.write_asi_checksum_string(file,string)
      checksum=Tools.checksum(string)
      file.write checksum.length.to_asi
      file.write checksum
      file.write string.length.to_asi
      file.write string
    end

    def Tools.to_asi_checksum_string(string)
      checksum=Tools.checksum(string)
      [checksum.length.to_asi,checksum,string.length.to_asi,string].join
    end

    def Tools.asi_checksum_string_prefix(string)
      checksum=Tools.checksum(string)
      checksum.to_asi_string+string.length.to_asi
    end

    def Tools.checksum(str)
      # MD5 hash
#      hashfunc = Digest::MD5.new
#      hashfunc.update(str)
#      hashfunc.hexdigest.force_encoding("BINARY") # place holder
      # crc32 checksum, little-endian encoded into 4 bytes
      [Zlib.crc32(str)].pack("V")
    end

    def Tools.checksum_array(str_array)
      # MD5 hash
#      hashfunc = Digest::MD5.new
#      hashfunc.update(str)
#      hashfunc.hexdigest.force_encoding("BINARY") # place holder
      # crc32 checksum, little-endian encoded into 4 bytes
      v=0
      str_array.each do |str|
        v = v ^ Zlib.crc32(str)
      end
      [v].pack("V")
    end

    def Tools.chunkify_directory(path,save_filename)
      load_directory(path).save(save_filename)
    end

    # target should implement the Monotable API
    # can be a chunk, localstore, etc.
    # if target is nil, a MemoryChunk is created (and returned)
    # target is returned
    # key_prefix is prepended onto every key inserted
    def Tools.load_directory(path,target=nil,key_prefix=nil)
      target||=MemoryChunk.new
      bytes_loaded=0
      Dir.glob(File.join(path,"**")) do |key|
        continue if File.stat(key).directory?
        value=nil
        target_key=key_prefix.to_s+File.basename(key)
        File.open(key,"rb") {|f| value=f.read.force_encoding("BINARY")}
        target.set(target_key,{"file_data"=>value})
      end
      target
    end
  end
end
