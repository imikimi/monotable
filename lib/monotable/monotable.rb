require File.join(File.dirname(__FILE__),"xbd")
require File.join(File.dirname(__FILE__),"version")

require 'yaml'
require 'cgi'
require 'fileutils'
require 'json'
require 'rubygems'
require 'eventmachine'
require 'em-http-request'
require 'evma_httpserver'
require 'uri'
require "addressable/uri"
require "fiber"
require "rest-client"
require 'goliath'
require 'em-synchrony/em-http'

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
  client/server_client
  exceptions/exceptions
  solo_daemon/solo_daemon
  router/router
}

monotable_require :server, %w{
  cluster_manager
  load_balancer
  server
  routes
  request_handler
  server_controller
  record_request_handler
}



monotable_require :goliath_server, %w{
  params_and_body
  goliath_server
}

monotable_require :event_machine_server, %w{
  event_machine_server
}

module Monotable
  # Your code goes here...
end
