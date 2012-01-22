require File.join(File.dirname(__FILE__),"xbd")
require File.join(File.dirname(__FILE__),"version")

def monotable_require(relative_path,modules)
  modules.each do |mod|
    require File.join(File.dirname(__FILE__),relative_path.to_s,mod)
  end
end

monotable_require :local_store, %w{
  api
  string
  global
  mini_event_machine
  cache
  record_cache
  index_block_cache
  constants
  tools
  file_handle
  journal
  journal_manager
  compaction_manager
  logger
  column
  columns
  record
  global_index
  chunk
  index_block
  index_block_encoder
  memory_chunk
  disk_chunk_base
  disk_chunk
  path_store
  local_store
}

monotable_require '', %w{
  server/cluster_manager
  server/load_balancer
  exceptions/exceptions
  solo_daemon/solo_daemon
  router/server_client
  router/router
}


require 'yaml'
require 'cgi'
require 'fileutils'
require 'json'
require 'rubygems'
require 'eventmachine'
require 'evma_httpserver'
require 'uri'
require "addressable/uri"

monotable_require :daemon, %w{
  daemon
  http_request_handler
  server_controller
  http_record_request_handler
  http_internal_request_handler
}

module Monotable
  # Your code goes here...
end
