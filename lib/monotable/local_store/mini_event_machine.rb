class MiniEventMachine
  class << self
    def init
      @queue=Queue.new
      @threads=[]
    end

    def queue(&task)
      @queue<<task
    end

    def defer(async_task,post_async_task)
      @threads<<[
        Thread.new(async_task) {|async_task|async_task.call},
        post_async_task
        ]
    end

    def check_threads
      @threads=@threads.select do |thread_structure|
        thread,post_task=thread_structure
        if thread.status
          true  # select and keep running threads
        else
          res = thread.join
          MiniEventMachine.queue {post_task.call(res)}
          false # remove done threads from list
        end
      end
    end

    # execute all blocks in the queue
    def process_queue
      check_threads
      while @queue.length>0
        @queue.pop.call
      end
    end

    def wait_for_all_tasks
      while @threads.length>0 || @queue.length>0
        process_queue
        sleep 0.01
      end
    end
  end
  MiniEventMachine.init
end
