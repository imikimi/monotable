#!/usr/bin/env ruby
require 'rubygems'
require 'optparse'
require 'ostruct'

class OptionsParser
  attr_accessor :options

  def initialize(args)
    @args=args
    @options={:verbose=>true}
    @parser = OptionParser.new do |opts|
      opts.banner = "Usage: #{__FILE__} [options]"
      opts.separator ""
      opts.separator "Options:"

      opts.on("-s","--store_paths list", Array, "Local store paths") do |list|
        options[:store_paths]=list
      end

      opts.on_tail("--help", "Show this message") do
        show_usage
      end

      opts.on("-q", "--quiet", "Silence output") {|p| options[:verbose]=false}
      opts.on("-p", "--port #", "Port number to listen to") {|p| options[:port]=p.to_i}
      opts.on("-h", "--host address", "Host address to listen to") {|h| options[:host]=h}
      opts.on("--initialize", "Initialize new store.") {|h| options[:initialize_new_multi_store]=true}

      opts.on_tail("--version", "Show version") do
        puts Monotable::VERSION
        exit
      end
    end
  end

  def parse
    non_options=@parser.parse!(@args)
    show_usage("Only options allowed. Please don't include: #{non_options.join(" ")}") if non_options.length>0
    validate
    @options
  rescue StandardError => e
    show_usage(e.to_s)
  end

  def show_usage(message=nil)
    puts @parser
    puts "\nOptions parsed: #{options.inspect}"
    puts "\nError: #{message}" if message
    exit
  end

  def validate
    unless options[:store_paths]
      show_usage "store_paths required"
    end
    options[:store_paths].each do |path|
      show_usage "store path #{path.inspect} does not exist" unless File.exists?(path)
      show_usage "store path #{path.inspect} is not a directory" unless File.stat(path).directory?
    end
  end

end

options=OptionsParser.new(ARGV).parse

options[:store_paths].each do |path|
  Dir.mkdir(path) unless File.exists?(path)
end

require File.expand_path(File.join(File.dirname(__FILE__),'..','lib','monotable','monotable.rb'))
begin
#  Monotable::GoliathServer::HttpServer.start(options)
  Monotable::EventMachineServer::HttpServer.start(options)
rescue Monotable::UserInterventionRequiredError => user_error
  $stderr.puts "\n#{user_error.class}:\n  #{user_error.to_s.gsub("\n","\n  ")}"
end
