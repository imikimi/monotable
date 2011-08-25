require File.join(File.dirname(__FILE__),"xbd")
require File.join(File.dirname(__FILE__),"version")

require File.join(File.dirname(__FILE__),'daemon')
require File.join(File.dirname(__FILE__),'daemon', 'record_deferrable')

%w{
  mini_event_machine
  cache
  constants
  tools
  file_handle
  journal
  journal_manager
  compaction_manager
  logger
  record
  chunk
  index_block
  index_block_encoder
  memory_chunk
  disk_chunk_base
  disk_chunk2
  path_store
  local_store
  }.each do |file|
    require File.join(File.dirname(__FILE__),"local_store",file)
end

%w{
  solo_daemon
  }.each do |file|
    require File.join(File.dirname(__FILE__),"solo_daemon",file)
end

module Monotable
  # Your code goes here...
end
