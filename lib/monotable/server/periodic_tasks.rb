module Monotable
  class PeriodicTasks < TopServerComponent

    # in seconds
    def task_periods
      {
      :local_store_balancer => 5*60,
      :heartbeat => 10,
      }
    end

    def active_tasks
      @active_tasks ||= {}
    end

    def start_task(name)
      Log.info "periodic task #{name}: started with period #{task_periods[name]} seconds"
      active_tasks[name] ||= EventMachine::PeriodicTimer.new(task_periods[name]) do
        start_time = Time.now
        yield
        Log.info "periodic task #{name}: took #{Time.now-start_time} seconds"
      end
    end

    def start
      # if we dont have the :break timer, then you can't control-c the daemon
      active_tasks[:break] ||= EventMachine::PeriodicTimer.new(0.1) {}

      start_task(:local_store_balancer) {local_store.balance_path_stores}
      start_task(:heartbeat) {}
    end

    def stop
      @active_tasks.each do |name,timer|
        timer.cancel
      end
      @active_tasks = {}
    end
  end
end
