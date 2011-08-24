module Monotable
  class Log
    class << self
      def <<(level,info=nil)
        level,info = :info,level unless info
        info = "#{Time.now.to_s}: #{info}"
        print info
#        log level,info
      end

      def print(info)
        puts info
      end

      def log(level,info)
        filename="#{level}.log"
        File.open filename,"rb" do |file|
          file.puts info
        end
      end
    end
  end
end
