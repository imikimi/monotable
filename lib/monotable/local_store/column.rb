module Monotable
  class Column
    attr_accessor :name, :properties

    # props can just be a string - the name of the column
    #   or a hash with at least the "name" field set
    def initialize(props={})
      case props
      when String then
        self.properties = {"name" => props}
      when Hash then
        self.properties = props
      else
        raise ArgumentError.new("string or hash required. got: #{props.inspect} (#{props.class})")
      end
      self.name=self["name"]
      raise ArgumentError.new("name property required") unless self.name
    end

    def [](prop_name) @properties[prop_name] end
    def []=(prop_name,val) @properties[prop_name]=val end

    def hash; name.hash; end
    def to_s; name; end

    def <=>(b) name<=>b.name; end
    include Comparable
  end
end
