module Monotable
  # bug / internal fault
  class InternalError < RuntimeError; end

  # client fault
  class ArgumentError < ::ArgumentError; end

  # temporary fault
  class TemporaryError < StandardError; end

  # network error
  # a remote machine didn't respond or responded incorreclty
  # "It's not my fault!"
  class NetworkError < StandardError; end
end
