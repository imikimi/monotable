#!/usr/bin/env ruby
require 'rubygems'
require 'trollop'
require File.expand_path(File.join(File.dirname(__FILE__),'..','lib','monotable','version.rb'))

def trollop_opts_parser(args)
  Trollop::options(args) do
    version v="Monotable Daemon v#{Monotable::VERSION} (c) Imikimi LLC (see LICENCE.TXT)"
    banner <<ENDBANNER
#{v}

Purpose: Start the Monotable Daemon

Usage:

  monotable [options]

Options:
ENDBANNER
    opt :quiet, "Silence output"
    opt :port, "Port number to listen to"
    opt :host, "host address to listen to"
    opt :verbose, "Verbose output"
    opt :initialize, "initialize a new store"
    opt :store_paths, "one or more local paths to store data (required)", :type => :strings
  end.tap do |opts|
    opts[:initialize_new_store] = opts[:initialize]
    Trollop::die :store_paths, "At least one store_path required." unless opts[:store_paths] && opts[:store_paths].length>0
    opts[:store_paths].each do |path|
      Trollop::die :store_paths, "store path #{path.inspect} does not exist" unless File.exists?(path)
      Trollop::die :store_paths, "store path #{path.inspect} is not a directory" unless File.stat(path).directory?
    end
  end
end

options=trollop_opts_parser(ARGV)

puts "Loading Monotable..."
require File.expand_path(File.join(File.dirname(__FILE__),'..','lib','monotable','monotable.rb'))

puts "Monotable internal initialization..."

begin
  Monotable::GoliathServer::HttpServer.start(options) do |server|
    server.periodic_tasks.start
  end
#  Monotable::EventMachineServer::HttpServer.start(options)
rescue Monotable::UserInterventionRequiredError => user_error
  $stderr.puts "\n#{user_error.class}:\n  #{user_error.to_s.gsub("\n","\n  ")}"
end
