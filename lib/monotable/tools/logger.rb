module Monotable
  class Log
    class << self
      attr_accessor :log_path

      def info(str) log(:info,str) end
      def debug(str) log(:debug,str) end
      def warn(str) log(:warn,str) end
      def error(str) log(:error,str) end
      def fatal(str) log(:fatal,str) end

      def verbose=(mode)
        @verbose=mode
      end

      def <<(level,info=nil)
        level,info = :info,level unless info
        info = "#{Process.pid}:#{Time.now.to_s}: #{info}"
        log level,info
      end

      def log_basename(level)
        "#{Time.now.to_s.split(' ',2)[0]}_monotable_#{level}.log"
      end

      def log_filename(level)
        File.join(log_path,log_basename(level))
      end

      def set_default_log_path
        unless log_path
          @log_path=Dir.pwd
          Tools.debug :log_path => nil, :using => @log_path
        end
      end

      def log(level,info)
        info = info.inspect unless info.kind_of?(String)
        set_default_log_path

        filename = log_filename(level)

        unless File.exists? filename
          Tools.debug "creating log: #{filename}"
        end

        puts info if @verbose

        File.open filename,"a" do |file|
          file.puts info
        end
      end
    end
  end
end
