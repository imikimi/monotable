#!/usr/bin/env ruby
require File.join(File.dirname(__FILE__),"..","lib","monotable","monotable")

#def package_directory(path,filename)
#  MobiusFS::Packager.package_directory(path,filename)
#end

class MTTChunk
  attr_accessor :chunk

  def initialize(filename)
    #time=Time.now
    @chunk=MonoTable::DiskChunk.new(:filename=>filename)
    #puts "DiskChunk init: #{Time.now-time}"
  end

  def info
    puts "Info:"
    puts chunk.info
  end

  def inspect_index(block=nil,depth=1)
    block||=@chunk.top_index_block
    indent="#{depth}#{"  "*(depth-1)}"
    puts indent+block.inspect
    if block.leaf?
      block.index_records.each do |key,ir|
        puts indent+" | "+ir.inspect
      end
    else
      block.index_records.each do |key,ir|
        inspect_index(block.sub_index_block(ir),depth+1)
      end
    end
  end

  def ls
    chunk.each_key do |key|
      puts key.inspect
    end
  end

  def get(key,field_names=nil)
    records=@chunk.get(key)
    if !records
      puts "record for key #{key.inspect} does not exists"
    elsif field_names && field_names.length>0
      field_names.each do |field_name|
        puts "#{field_name.inspect} => #{records[field_name].inspect}"
      end
    else
      puts "field-names: "+records.keys.inspect
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

  ii chunk_file

    inspect index

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
  when "ii"         then required_args(args,1,"filename");      MTTChunk.new(args[1]).inspect_index
  when "ls","list"  then required_args(args,1,"filename");      MTTChunk.new(args[1]).ls
  when "g","get"    then required_args(args,2,"filename key");  MTTChunk.new(args[1]).get(args[2],args[3..-1])
  when "i","info"   then required_args(args,1,"filename");      MTTChunk.new(args[1]).info
  else show_usage
  end

end

tool(ARGV)
