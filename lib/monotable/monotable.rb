
require 'eventmachine'
require 'em-http-request'
require 'evma_httpserver'
require 'uri'
require "addressable/uri"
require "fiber"
require "rest-client"
require 'goliath'
require 'em-synchrony'
require 'em-synchrony/em-http'

require File.join(File.dirname(__FILE__),"tools.rb")

monotable_require '', %w{
  local_store
  client/server_client
  exceptions/exceptions
  server
}

monotable_require :goliath_server, %w{
  params_and_body
  goliath_server
}

monotable_require :event_machine_server, %w{
  event_machine_server
}
