require "rubygems"
require "babel_bridge"

class CodeMarkup < BabelBridge::Parser
  rule :file, many(:element) do 
    def markup
      "<pre><code>"+
      element.collect{|a| a.markup}.join.strip+
      "</code></pre>" 
    end
  end
  
  rule :element, "<", :space do
    def markup; "<symbol>&lt;</symbol>#{space}" end
  end

  rule :element, ">", :space do
    def markup; "<symbol>&gt;</symbol>#{space}" end
  end
  
  rule :element, :comment, :space do
    def markup; "<comment>#{comment}</comment>#{space}" end
  end
  
  rule :element, :keyword, :space do
    def markup; "<keyword>#{keyword}</keyword>#{space}" end
  end
  
  rule :element, :string, :space do
    def markup
      str=string.to_s.gsub("<","&lt;").gsub(">","&gt;")
      "<string>#{str}</string>#{space}" 
    end
  end

  rule :element, :regex, :space do
    def markup; "<regex>#{regex}</regex>#{space}" end
  end
  
  rule :element, :identifier, :space do
    def markup; "<identifier>#{identifier}</identifier>#{space}" end
  end
  
  rule :element, :symbol, :space do
    def markup; "<symbol>#{symbol}</symbol>#{space}" end
  end
  
  rule :element, :number, :space do
    def markup; "<number>#{number}</number>#{space}" end
  end
  
  rule :element, :non_space, :space do
    def markup; "#{non_space}#{space}" end
  end
  
  rule :space, /\s*/
  rule :number, /[0-9]+(\.[0-9]+)?/
  rule :comment, /#[^\n]*/
  rule :string, /"(\\.|[^\\"])*"/
  rule :string, /:[_a-zA-Z0-9]+[?!]?/
  rule :regex, /\/(\\.|[^\\\/])*\//
  rule :symbol, /[-!@\#$%^&*()_+={}|\[\];:<>\?,\.\/~]+/
  rule :keyword, /class|end|def|and|or|do|if|then/
  rule :keyword, /else|elsif|case|then|when|require/
  rule :identifier, /[_a-zA-Z][0-9_a-zA-Z]*/
  rule :non_space, /[^\s]+/
end

puts CodeMarkup.new.parse(File.read(ARGV[0])).markup