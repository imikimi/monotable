module Monotable
  class CompactionManager

    class << self
      def init
        @queue=[]
      end

      attr_reader :currently_compacting

      def queued_compactions
        @queue.collect {|a| a[:journal_file]}
      end

      def start_next_compaction
        if @queue.length>0
          info=@queue.shift
          @currently_compacting=info[:journal_file] || true # || true -> ensure correct operation of CompactionManger even if journal_file is nil
          EventMachine.defer(info[:async_task],info[:post_task])
        else
          @currently_compacting=false
        end
      end

      def queue_compaction(async_task,post_task,journal_file)
        Log << "CompactionManager.queue_compaction(#{journal_file.inspect})"
        if @currently_compacting
          @queue << {:async_task=>async_task,:post_task=>post_task,:journal_file=>journal_file}
        else
          @currently_compacting=journal_file || true # || true -> ensure correct operation of CompactionManger even if journal_file is nil
          EventMachine.defer(async_task,post_task)
        end
      end

      def compact(journal_file,&block)
        async_task = Proc.new do
          Tools.log_time("async:Journal.compact_phase_1_external(#{journal_file.inspect})",true) do
            Journal.compact_phase_1_external(journal_file)
          end
        end

        post_task = Proc.new do
          Tools.log_time("sync:Journal.compact_phase_2(#{journal_file.inspect})") do
            Journal.compact_phase_2(journal_file)
            yield if block
          end
          start_next_compaction
        end

        queue_compaction(async_task,post_task,journal_file)
      end
    end
  end
  CompactionManager.init
end
