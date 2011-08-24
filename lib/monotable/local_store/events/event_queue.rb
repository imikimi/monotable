require 'thread'

module Monotable
  class EventQueue

    class <<self
      def clear
        @queue = Queue.new
      end

      def empty?;   @queue.empty?; end
      def length;   @queue.length; end
      def pop;      @queue.pop; end
      def push(a);  @queue.push(a); end

      alias size length
      alias deq pop
      alias shift pop
      alias enq push
      alias << push
    end
  end
  EventQueue.clear
end
