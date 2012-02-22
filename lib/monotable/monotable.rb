require File.join(File.dirname(__FILE__),"local_store")

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

monotable_require :patches, %w{
  eventmachine
}

monotable_require '', %w{
  client/server_client
  exceptions/exceptions
}

monotable_require :server, %w{
  top_server_component
  router
  global_index
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
