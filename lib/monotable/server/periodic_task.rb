module Monotable
  class PeriodicTask < TopServerComponent

    def active_tasks
      @active_tasks ||= {}
    end

    # in seconds
    def task_periods
      {
      :local_store_balancer => 5*60
      }
    end

    def start_task(name)
      @active_tasks[name] ||= EventMachine::PeriodicTimer.new(task_periods[name]) do
        start_time = Time.now
        Log << "Periodic task #{name.inspect} start..."
        yield
        Log << "Periodic task #{name.inspect} done. Took #{Time.now-start_time} seconds."
      end
    end

    def start_tasks
      start_task(:local_store_balancer) {local_store.balance_path_stores}
    end
  end
end
