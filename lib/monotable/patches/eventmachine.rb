module Monotable
  module EventMachineRunning
    # expose the "running" state of eventmachine
    attr_reader :running
  end
end

module EventMachine
  class << self
    include Monotable::EventMachineRunning
  end
end
