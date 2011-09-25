require File.join(File.dirname(__FILE__),"xbd")
require File.join(File.dirname(__FILE__),"version")

def mt_require(relative_path,modules)
  modules.each do |mod|
    require File.join(File.dirname(__FILE__),relative_path.to_s,mod)
  end
end

mt_require :local_store, %w{
  api
  string
  global
  mini_event_machine
  cache
  record_cache
  index_block_cache
  constants
  global_index
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
  disk_chunk
  path_store
  local_store
}

mt_require '', %w{
  exceptions/exceptions
  solo_daemon/solo_daemon
  router/server_client
  router/router
}


mt_require :daemon, %w{
  daemon
  http_request_handler
  http_record_request_handler
  http_internal_request_handler
}

module Monotable
  # Your code goes here...
end
