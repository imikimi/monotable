module Xbd
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
    attr_reader :attrs
    def [](attr) @attrs[attr.to_s] end
    def []=(attr,val) val==nil ? @attrs.delete(attr.to_s) : @attrs[attr.to_s]=val.to_s end

    #************************************************************
    # Access Tags
    #************************************************************
    # return tags array
    attr_reader :tags
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

    def each_attribute
      @attrs.each {|k,v| yield k,v}
    end

    # iterate over all tags or only with matching names
    def each_tag(name=nil)
      tags.each do |tag|
        yield tag if !name || tag.name==name
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

    def ==(tag)
      name==tag.name &&
      attrs==tag.attrs &&
      tags==tag.tags
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

    # encode just this tag in binary
    # Note this returned value alone is not parsable
    def to_binary_partial(tagsd,attrsd,valuesd)
      # build attrs_data string: all attr name-value pairs as ASIs concatinated
      attrs_data=attrs.keys.sort.collect {|key| attrsd[key].to_asi + valuesd[attrs[key]].to_asi}.join

      data=tagsd[name].to_asi +                                         # name asi
        attrs_data.length.to_asi + attrs_data +                         # attrs length asi and attrs
        tags.collect {|tag| tag.to_binary_partial(tagsd,attrsd,valuesd)}.join   # sub-tags
      data.to_asi_string                                                # tag data pre-pended with tag-data length asi
    end

    #************************************************************
    # to binary XBD (to_xbd)
    #************************************************************
    # use this to convert an xbd tag structure into a saveable xbd file-string
    def to_binary
      populate_dictionaries(tagsd=Dictionary.new, attrsd=Dictionary.new, valuesd=Dictionary.new)
      Xbd::SBDXML_HEADER + tagsd.to_binary + attrsd.to_binary + valuesd.to_binary + to_binary_partial(tagsd,attrsd,valuesd)
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
end
