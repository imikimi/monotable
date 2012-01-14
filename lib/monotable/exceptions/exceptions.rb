module Monotable
  # bug / internal fault
  class InternalError < RuntimeError; end

  # client fault
  class ArgumentError < ::ArgumentError; end

  # temporary fault
  class TemporaryError < StandardError; end

  # signal that a human needs to do something to resolve this error
  class UserInterventionRequiredError < StandardError; end

  # network error
  # a remote machine didn't respond or responded incorreclty
  # "It's not my fault!"
  class NetworkError < StandardError; end
end
