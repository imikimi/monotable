module Monotable
  class Column < Hash
    attr_accessor :name, :properties

    # props can just be a string - the name of the column
    #   or a hash with at least the "name" field set
    def initialize(props={})
      case props
      when String then
        props =  {"name" => props}
      when Hash then
      else
        raise ArgumentError.new("string or hash required. got: #{props.inspect} (#{props.class})")
      end
      self.merge! props
      self.name=self["name"]
      raise ArgumentError.new("name property required") unless self.name
    end

    def to_s; name; end

    def inspect; "Column:#{name.inspect}";end
  end
end
