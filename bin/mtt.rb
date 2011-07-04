#!/usr/local/bin/ruby
require File.join(File.dirname(__FILE__),"..","lib","monotable","monotable")

#def package_directory(path,filename)
#  MobiusFS::Packager.package_directory(path,filename)
#end

class MTTChunk
  attr_accessor :chunk

  def initialize(filename)
    #time=Time.now
    @chunk=MonoTable::DiskChunk.new(filename)
    #puts "DiskChunk init: #{Time.now-time}"
  end

  def info
    puts "Info:"
    puts chunk.info
  end

  def ls
    chunk.each_key do |key|
      puts key.inspect
    end
  end

  def save_value(key,to_filename=nil)
    to_filename||=key
    value=chunk[key]
    if value
      puts "Writing #{value.length} bytes to #{to_filename}"
      File.open(to_filename,"wb") {|f| f.write(value)}
    else
      puts "key #{key.inspect} not found in #{chunk.filename.inspect}"
    end
  end
end

def show_usage(message=nil)
  puts <<ENDUSAGE
MonoTable Tool

Usage: #{__FILE__} (mode) (options)

Modes:

  ls chunk_file
  list chunk_file

    List the keys in a chunk

  g chunk_file key [save_filename]
  get chunk_file key [save_filename]

    Get the value for a key, writes it to a file.
    Default save_filename is the key.

  i chunk_file
  info chunk_file

    Show the info-block.

ENDUSAGE
  puts message if message
  exit
end

def required_args(args,count,args_info)
  if args.length-1 < count
    show_usage("#{count} args expect: #{args_info}")
  end
end

def tool(args)
  case args[0]
  when "ls","list"  then required_args(args,1,"filename");      MTTChunk.new(args[1]).ls
  when "g","get"    then required_args(args,2,"filename key");  MTTChunk.new(args[1]).get(args[2],args[3])
  when "i","info"   then required_args(args,1,"filename");      MTTChunk.new(args[1]).info
  else show_usage
  end

end

tool(ARGV)
