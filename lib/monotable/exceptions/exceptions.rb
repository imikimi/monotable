module Monotable
  # bug / internal fault
  # Implication:
  #   Monotable development is responsible for fixing
  class InternalError < RuntimeError; end

  # Implication:
  #   Client calling the method passed in the wrong arguments;
  #   Client's responsibility to fix.
  class ArgumentError < ::ArgumentError; end

  # temporary fault
  # Implication:
  #   Retry
  class TemporaryError < StandardError; end

  # Implication:
  #   User (admin) needs to do something to resolve this error
  class UserInterventionRequiredError < StandardError; end

  # network error
  # a remote machine didn't respond or responded incorreclty
  # "It's not my fault!"
  # Implications are:
  #   1) Retry OK; may solve the problem
  #   2) Server may need to be marked as down if repeatadly failing
  class NetworkError < TemporaryError; end

  # if the monotable datastructure is in an unexpected state...
  # Implication:
  #   User (admin) needs to do something to resolve this error
  class MonotableDataStructureError < UserInterventionRequiredError; end
end
