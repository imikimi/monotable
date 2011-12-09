module Monotable
  class Columns
    attr_reader :columns,:columns_by_id

    def initialize(from_xbd=nil)
      @columns={}
      @columns_by_id=[]
      load_xbd(from_xbd) if from_xbd
    end

    def load_xbd(xbd_tag)
      xbd_tag.each_tag do |tag|
        col_info={}
        tag.each_attribute do |k,v|
          col_info[k]=v
        end
        add col_info
      end
    end

    def add(column_properties)
      @columns[column_properties]||= begin
        id = @columns_by_id.length
        @columns_by_id<<column_properties
        id
      end
    end

    def inspect; xbd_tag.to_xml; end

    def length;@columns.length; end

    def each; @columns_by_id.each {|col| yield col} end
    def each_with_index; @columns_by_id.each_with_index {|col,i| yield col,i} end

    def [](key) @columns[key] || @columns_by_id[key] end

    def ==(other) @columns_by_id == other.columns_by_id; end

    def xbd_tag
      Xbd::Tag.new("columns") do |tag|
        each do |col_info|
          tag<<Xbd::Tag.new("column") do |col_tag|
            col_info.each {|k,v| col_tag[k]=v}
          end
        end
      end
    end

  end
end
