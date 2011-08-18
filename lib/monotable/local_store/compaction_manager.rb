module MonoTable
  class CompactionManager

    class CompactorProcess
      attr_accessor :state
      attr_accessor :exception
      attr_accessor :journal_file
      attr_accessor :start_time,:end_time
      attr_accessor :thread_state

      def initialize(journal_file)
        @journal_file=journal_file.to_s
        @thread_state=:not_done
      end

      def to_s
        journal_file
      end

      def end_time; @end_time || Time.now; end
      def async_time; end_time-start_time; end

      def run
        begin
          @start_time=Time.now
          self.state=:running
          log
          Journal.compact_phase_1_external(self.journal_file)
          CompactionManager.queue(self) do
            Journal.compact_phase_2(self.journal_file)
          end
          self.state=:phase_1_success
        rescue Exception=>e
          self.state=:failure
          self.exception=e
          CompactionManager.queue(self) do
            raise e
          end
        ensure
          @end_time=Time.now
          @thread_state=:done
          log
        end
      end

      def hash
        state.hash
      end

      def run_timed_post_block
        @post_start_time=Time.now
        yield
        state=:done
        @post_end_time=Time.now
      end

      def post_time
        @post_end_time-@post_start_time rescue nil
      end

      def log(info=nil)
        Log << "CompactorProcesses(#{journal_file.inspect}) #{info} :thread_state=>#{thread_state.inspect}, :state=>#{state.inspect}, :async_time => #{async_time}, :post_time => #{post_time}"
      end
    end

    class CompactionThread
      def initialize
        @queue=Queue.new
        thread_loop
      end

      def thread_loop
        @thread||=Thread.new(self) do |ct|
          while true
            begin
              if @queue.length>0
                @queue.pop.run
              else
                Thread.stop
              end
            rescue Exception => e
              Tools.log_error e
            end
          end
        end
      end

      def compact(journal_file)
        cp=CompactorProcess.new(journal_file)
        @queue<<cp
        @thread.run
        cp
      end
    end

    class <<self
      def initialize
        @active_compactors={}
        @active_compactors_lock=Mutex.new
        @queue=Queue.new
        @compaction_thread=CompactionThread.new
      end

      def compact(journal_file)
        puts "#{self.class}#compact #{journal_file.inspect}"
        @active_compactors_lock.synchronize do
          (@active_compactors[journal_file.to_s]=@compaction_thread.compact(journal_file)) unless @active_compactors[journal_file.to_s]
        end
      end

      def compactor_states
        @active_compactors_lock.synchronize do
          @active_compactors.collect {|k,v| [k,v.thread_state,v.state]}
        end
      end

      def running_compactors
        @active_compactors_lock.synchronize do
          @active_compactors.select {|k,v| v.thread_state==:not_done}
        end
      end

      def wait_for_compactors
        while true
          if running_compactors.length>0 || @queue.length>0
            process_queue
            sleep 0.1
          else
            break
          end
        end
      end

      def queue(compactor,&block)
        @queue << [compactor,block]
      end

      # call this from the main thread
      def process_queue
        num_processed=0
        while @queue.length>0
          compactor,block=@queue.pop
          compactor.run_timed_post_block &block
          num_processed+=1
        end
        num_processed>0 && num_processed
      end
    end
  end
  CompactionManager.initialize
end
