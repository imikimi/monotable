module Monotable
  class Log
    class << self
      attr_accessor :log_path

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
        set_default_log_path

        filename = log_filename(level)

        unless File.exists? filename
          Tools.debug "creating log: #{filename}"
        end

        File.open filename,"a" do |file|
          file.puts info
        end
      end
    end
  end
end
