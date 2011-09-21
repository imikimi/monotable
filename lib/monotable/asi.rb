# encoding: BINARY
$ruby_inline=true
require "inline" if $ruby_inline
require "stringio"

module Xbd
  #*********************************
  # Xbd::Asi module
  #*********************************
  #
  # Read and Generate ASI strings
  class Asi

    # read an ASI from a string, returning an integer
    # optionally starts and the specified offset index.
    #
    # returns the number read and the first index after the ASI data in the string.
    def Asi.read_asi(source,index=0)
      ret=0
      shift=0
      val=0
      while index<source.length
        val=source.byte(index)
        ret+= (val & 0x7F) << shift;
        shift+=7
        index+=1
        break if val<128
      end
      return ret,index
    end

    def Asi.read_asi_string(source,index=0)
      n,index=read_asi(source,index)
      return source[index,n],index+n
    end

    def Asi.read_asi_from_file(file)
      ret=0
      shift=0
      val=0
      while val=file.readbyte
        ret+= (val & 0x7F) << shift;
        shift+=7
        break if val<128
      end
      return ret
    end

    def Asi.asi_to_i(source)
      Asi.read_asi(source,0)[0]
    end

    # this C function supports all values up to the maximum value of 2^64-1
    # RubyInline autodetects if the number is too big and throws an
    if $ruby_inline
    inline do |compiler|
      compiler.c <<-ENDC
        VALUE i_to_asi_c(unsigned long num) {
          char str[11]; // I think 10 is enough, but just to be safe
          int p=0;
          while(p==0 || num > 0) {
            int byte = (int)(num & 0x7F);
            num = num >> 7;
            if (num > 0) byte = byte | 0x80;
            str[p++]=byte;
          }
          return rb_str_new(str,p);
        }
      ENDC
    end
    else
      def i_to_asi_c(num)
        Asi.i_to_asi_ruby(num)
      end
    end

    ASI_INSTANCE=Asi.new
    def Asi.i_to_asi2(num)
      ASI_INSTANCE.i_to_asi_c(num)
    end

    def Asi.i_to_asi_ruby(num)
      ret=""
      while ret.length==0 || num>0
        val=num & 0x7F;
        num=num>>7
        val|=0x80 if num>0
        ret<<val
      end
      ret
    end
    class <<self
      alias :i_to_asi :i_to_asi_ruby
    end

    def Asi.asi_length(num)
      count=1
      while num>=0x80
        num>>=7
        count+=1
      end
      count
    end

  #*********************************
  # Enable ASI reading and writing
  # in standard objects.
  #*********************************
    module IO
      def read_asi(index=0)
        Asi.read_asi_from_file(self)
      end

      # read an asi and then read the next N bytes, where N is the asi value
      # index's value is ignored
      def read_asi_string(index=0)
        read(Asi.read_asi_from_file(self))
      end
    end

    module Fixnum
      ASI_INSTANCE = Asi.new
      def to_asi
        ASI_INSTANCE.i_to_asi_c(self)
      end
      def asi_length
        Xbd::Asi.asi_length(self)
      end
    end

    module Bignum
      def to_asi
        Asi.i_to_asi(self)
      end
      def asi_length
        Xbd::Asi.asi_length(self)
      end
    end

    module String
      def from_asi
        Asi.asi_to_i(self)
      end

      def to_asi_string
        self.length.to_asi+self
      end

      def read_asi(index=0)
        Asi.read_asi(self,index)
      end

      def read_asi_string(index=0)
        Asi.read_asi_string(self,index)
      end

      # Ruby 1.8 patch to ignore force_encoding
      if !"".respond_to?(:force_encoding)
        def to_binary; self end
        def force_encoding(a) self end
        def byte(index)
          self[index]
        end
      else
      # Ruby 1.9
        def to_binary; self.force_encoding("BINARY") end
        def byte(index)
          char=self[index]
          char && char.bytes.next
        end
      end
    end
  end
end

class Fixnum  ; include Xbd::Asi::Fixnum   ; end
class File    ; include Xbd::Asi::IO       ; end
class StringIO; include Xbd::Asi::IO       ; end
class Bignum  ; include Xbd::Asi::Bignum   ; end
class String  ; include Xbd::Asi::String   ; end
