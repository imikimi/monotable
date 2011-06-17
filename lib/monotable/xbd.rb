# encoding: BINARY
#*****************************************************
# Ruby XBD Library
# (C) 2010-10-03 Shane Brinkman-Davis
#
# SRC home:
#   URL: https://svn.imikimi.com/auto_branch/2008-10-30_15-54-25_iphone_edge/xbd/xbd.rb
#   Repository Root: https://svn.imikimi.com
#   Repository UUID: d1359c2d-ec2c-0410-b0bc-eca7d5e44040
#   Revision: 11647
#*****************************************************
=begin

This code read and writes XBD files.

To get started:

Take any XBD file
  example Kimi from Imikimi.com (all Kimis are XBD containers)
    http://imikimi.com/plugin/get_kimi/o-10i

And run:
  require "xbd.rb"
  puts Xbd.load_from_file("your_xbd_filename.xbd")

Master verison of this file lives in: imikimi_plugin_source/xbd/xbd.rb

=end

#*********************************
# Enable ASI reading and writing
# in standard objects.
#*********************************
class Fixnum
    def to_asi
        Xbd::Asi.i_to_asi(self)
    end
end

module Xbd
  module Asi
    module AsiIO
      def read_asi(index=0)
        Xbd::Asi.read_asi_from_file(self)
      end

      # read an asi and then read the next N bytes, where N is the asi value
      # index's value is ignored
      def read_asi_string(index=0)
        read(Xbd::Asi.read_asi_from_file(self))
      end
    end
  end
end

class File
  include Xbd::Asi::AsiIO
end

require "stringio"
class StringIO
  include Xbd::Asi::AsiIO
end

class Bignum
    def to_asi
        Xbd::Asi.i_to_asi(self)
    end
end

class String
  def from_asi
      Xbd::Asi.asi_to_i(self)
  end

  def to_asi_string
    self.length.to_asi+self
  end

  def read_asi(index=0)
    Xbd::Asi.read_asi(self,index)
  end

  def read_asi_string(index=0)
    Xbd::Asi.read_asi_string(self,index)
  end

  # Ruby 1.8 patch to ignore force_encoding
  if !"".respond_to?(:force_encoding)
    def force_encoding(a) self end
    def byte(index)
      self[index]
    end
  else
  # Ruby 1.9
    def byte(index)
      char=self[index]
      char && char.bytes.next
    end
  end
end

#*********************************
# Xbd Module
#*********************************
module Xbd

  SBDXML_HEADER="SBDXML\1\0"

  #**********************************************************
  # XML escaping
  #**********************************************************
  # Used when converting TO xml (Xbd::Tag.to_s)
  # Note, some of these codes are actually invalid strict XML because XML has no escape codes for some values.
  #   SBD: WTF! (I'm still surprised by this, but it appears to be true.)
  XML_ESCAPE_CODES={
    "&#0;"=>0,
    "&#1;"=>1,
    "&#2;"=>2,
    "&#3;"=>3,
    "&#4;"=>4,
    "&#5;"=>5,
    "&#6;"=>6,
    "&#7;"=>7,
    "&#8;"=>8,
    "&#9;"=>9,
    "&#10;"=>10,
    "&#11;"=>11,
    "&#12;"=>12,
    "&#13;"=>13,
    "&#14;"=>14,
    "&#15;"=>15,
    "&#16;"=>16,
    "&#17;"=>17,
    "&#18;"=>18,
    "&#19;"=>19,
    "&#20;"=>20,
    "&#21;"=>21,
    "&#22;"=>22,
    "&#23;"=>23,
    "&#24;"=>24,
    "&#25;"=>25,
    "&#26;"=>26,
    "&#27;"=>27,
    "&#28;"=>28,
    "&#29;"=>29,
    "&#30;"=>30,
    "&#31;"=>31,
    "&quot;"=>34,   #"
    "&amp;"=>38,    #&
    "&apos;"=>39,   #'
    "&lt;"=>60,     #<
    "&gt;"=>62      #>
  }

  @@escape_for_xml=[]
  (0..255).each {|i| @@escape_for_xml<<i.chr}
  XML_ESCAPE_CODES.each {|k,v| @@escape_for_xml[v]=k}

  def self.xml_escape(s)
    out=""
    s.each_byte {|b| out<< @@escape_for_xml[b]}
    out
  end

  #*********************************
  # Xbd::Asi module
  #*********************************
  #
  # Read and Generate ASI strings
  module Asi

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

    def Asi.i_to_asi(num)
      ret=""
      while ret.length==0 || num>0
        val=num & 0x7F;
        num=num>>7
        val|=0x80 if num>0
        ret<<val
      end
      ret
    end

    def Asi.asi_length(num)
      count=1
      while num>=0x80
        num>>7
        count+=1
      end
      count
    end
  end

  #*********************************
  # Xbd::Tag object
  #*********************************
  #
  # Consists of:
  #   name:     a string
  #   attrs:    a Hash of Attributes, String -> String
  #   tags:     an ordered Array of Tag objects
  class Tag

    def initialize(name,attrs=nil,tags=nil,&block)
      @name=name.to_s
      @attrs={}
      attrs.each {|k,v|@attrs[k.to_s]=v.to_s} if attrs
      @tags=[]
      self<<tags if tags
      yield self if block
    end
    #************************************************************
    # Access Name
    #************************************************************
    def name() @name end
    def name=(n) @name=n end

    #************************************************************
    # Access Attrs
    #************************************************************
    def attrs() @attrs end
    def [](attr) @attrs[attr.to_s] end
    def []=(attr,val) val==nil ? @attrs.delete(attr.to_s) : @attrs[attr.to_s]=val.to_s end

    #************************************************************
    # Access Tags
    #************************************************************
    # return tags array
    def tags() @tags end
    def tagnames() @tags.collect {|t| t.name} end

    # returns first tag that matches name
    # names can be a "/" delimited path string
    # OR an array of exact string values to match (in case you want to match "/" in a tag name)
    def tag(names)
      return self if !names || (names.kind_of?(Array) && names.length==0)
      names=names.split("/") unless names.kind_of?(Array)
      name=names[0]
      tags.each do |tag|
        return tag.tag(names[1..-1]) if tag.name==name
      end
      return nil
    end

    # iterate over all tags with matching names
    def each_tag(name)
      tags.each do |tag|
        yield tag if tag.name==name
      end
    end

    # Add sub-tag or array of sub-tags
    def <<(tag)
      return unless tag # ignore nil
      tags= tag.kind_of?(Array) ? tag : [tag]
      tags.each {|t| raise "All sub-tags in must be #{self.class} objects. Attempted to add #{t.class} object." unless t.kind_of?(self.class)}
      @tags+=tags
      tag
    end

    #************************************************************
    # to XML (to_s)
    #************************************************************
    def to_s(indent="",max_attr_value_length=nil)
      a=[name]
      attrs.keys.sort.each do |k|
        v=attrs[k]
        v=v[0..max_attr_value_length-1] if max_attr_value_length
        a<<"#{k}=\"#{Xbd.xml_escape(v)}\""
      end
      ret="#{indent}<#{a.join(' ')}"
      if tags.length>0
        ret+=">\n"
        tags.each {|st| ret+=st.to_s(indent+"    ",max_attr_value_length)}
        ret+="#{indent}</#{name}>\n"
      else
        ret+="/>\n"
      end
    end
    alias :to_xml :to_s

    def inspect
      to_s("",32)
    end

    # convert to basic ruby data structure
    def to_ruby
      {:name=>name, :attrs=>@attrs.clone, :tags=>tags.collect {|tag|tag.to_ruby}}
    end

    #************************************************************
    # to binary XBD support methods
    #************************************************************
    def populate_dictionaries(tagsd,attrsd,valuesd)
      tagsd<<name                                                       # add this tag's name
      attrs.each {|k,v| attrsd<<k; valuesd<<v}                           # add all attribute names and values
      tags.each {|tag| tag.populate_dictionaries(tagsd,attrsd,valuesd)} # recurse on sub-tags
    end

    def to_binary(tagsd,attrsd,valuesd)
      # build attrs_data string: all attr name-value pairs as ASIs concatinated
      attrs_data=attrs.keys.sort.collect {|key| attrsd[key].to_asi + valuesd[attrs[key]].to_asi}.join

      data=tagsd[name].to_asi +                                         # name asi
        attrs_data.length.to_asi + attrs_data +                         # attrs length asi and attrs
        tags.collect {|tag| tag.to_binary(tagsd,attrsd,valuesd)}.join   # sub-tags
      data.to_asi_string                                                # tag data pre-pended with tag-data length asi
    end

    #************************************************************
    # to binary XBD (to_xbd)
    #************************************************************
    # use this to convert an xbd tag structure into a saveable xbd file-string
    def to_xbd
      populate_dictionaries(tagsd=Dictionary.new, attrsd=Dictionary.new, valuesd=Dictionary.new)
      Xbd::SBDXML_HEADER + tagsd.to_binary + attrsd.to_binary + valuesd.to_binary + to_binary(tagsd,attrsd,valuesd)
    end

    #**********************************************************
    # Load XBD Tag Data from String
    #**********************************************************
    # parse a Tag, all its Attributes and all its Sub-tags recursively.
    #
    # inputs:
    #   source - source binary string
    #   index - offset to start reading at
    #   tagsd - tag-names dictionary
    #   attrsd - attribute-names dictionary
    #   valuesd - attribute-values dictionary
    # returns the Tag object generated AND the first "index" in the string after read tag-data
    def Tag.parse(source,index,tagsd,attrsd,valuesd)
      tag_length,index=Asi.read_asi(source,index)
      tag_start_index=index

      # read tag name
      tag_name_id,index=Asi.read_asi(source,index)
      tag_name=tagsd[tag_name_id]
      raise "tag name id(#{tag_name_id}) not in tag-names dictionary" if !tag_name

      # read attributes
      attr_byte_size,index=Asi.read_asi(source,index)
      attrs_hash={}
      while attr_byte_size>0
        i=index
        name_id,index=Asi.read_asi(source,index)
        value_id,index=Asi.read_asi(source,index)
        attr_byte_size-=(index-i)
        n=attrsd[name_id]
        v=valuesd[value_id]
        raise "attribute name id(#{name_id}) not in attribute-names dictionary" if !n
        raise "attribute value id(#{value_id}) not in attribue-values dictionary" if !v
        attrs_hash[n]=v
      end
      tag_length-=(index-tag_start_index)

      # read sub-tags
      tags=[]
      while tag_length>0
        i=index
        node,index=Tag.parse(source,index,tagsd,attrsd,valuesd)
        tags<<node
        tag_length-=(index-i)
      end
      return Tag.new(tag_name,attrs_hash,tags),index
    end
  end

  #*********************************
  # Xbd::Dictionary
  #*********************************
  # Consists of:
  #   @hash:  a map from values to IDs and IDs to values
  #   @array: a list of values; their indexes == their IDs
  #
  class Dictionary

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
    def Dictionary.parse(source,index)
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

  # XBD.parse accepts:
  #   a string or
  #   any object that returns a string in response to the method "read"
  #     (for example, an open file handle)
  def Xbd.parse(source)
    #treat source as a stream(file) if it isn't a string
    source=source.read.force_encoding("BINARY") if source.class!=String

    # read the header
    raise "Not a valid XBD file" unless source[0..SBDXML_HEADER.length-1]==SBDXML_HEADER
    index=SBDXML_HEADER.length

    # read each of the 3 dictionaries in order
    tagsd,index=Dictionary.parse(source,index)
    attrsd,index=Dictionary.parse(source,index)
    valuesd,index=Dictionary.parse(source,index)

    # read all tags, return the root-tag
    Tag.parse(source,index,tagsd,attrsd,valuesd)[0]
  end

  # Load XBD from filename
  def Xbd.load_from_file(filename)
    Xbd.parse(File.open(filename,"rb"))
  end

  # loads an XBD file, decodes it, encodes it, decodes it and compares the two decodes
  #   It isn't effective to compare the two encodings because the dictionaries may be constructed in different orders
  #   It IS effective to compare the pseudo XML output as Tag order must be preserved, and though Attribute order is irrelevent, the library forces a consistent ordering in the to_s metod
  def Xbd.test_decode_encode(filename)
    file_data=File.read(filename).force_encoding("BINARY")
    puts "file_data: #{file_data.length}"
    decoded1=Xbd.parse(file_data)
    puts "decoded: #{decoded1.to_s.length}"
    encoded=decoded1.to_xbd
    puts "encoded: #{encoded.length}"
    decoded2=Xbd.parse(encoded)
    puts "decoded2: #{decoded2.to_s.length}"
    puts "decoded2.to_ruby: #{decoded2.to_ruby.inspect.length}"
    raise "match failed" unless decoded1.to_ruby==decoded2.to_ruby
  end
end
