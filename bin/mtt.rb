#!/usr/bin/env ruby
require File.expand_path(File.join(File.dirname(__FILE__),"..","lib","monotable","local_store"))
require "trollop"

#def package_directory(path,filename)
#  MobiusFS::Packager.package_directory(path,filename)
#end

class MTTChunk
  attr_accessor :chunk
  attr_accessor :indent_string

  def iputs(out)
    puts (@indent_string+out.to_s.gsub("\n","\n#{@indent_string}"))
  end

  # pass a block and indent all output within that block
  def indent(label=nil)
    iputs label if label
    old_indent = @indent_string
    @indent_string+="  "
    yield
    @indent_string = old_indent
  end

  def initialize(filename)
    raise "chunk #{filename.inspect} not found" unless File.exists?(filename)
    @indent_string=""
    #time=Time.now
    @chunk=Monotable::DiskChunk.new(:filename=>filename)
    #puts "DiskChunk init: #{Time.now-time}"
  end

  def basics(options={})
    status = @chunk.status
    status[:file_size] = @chunk.file_handle.size
    status[:columns] = @chunk.columns.array.sort
    units = {accounting_size: :bytes, file_size: :bytes}
    fields = [:record_count,:accounting_size,:file_size,:range_start,:range_end, :columns]
    if options[:keys]
      fields << :keys
      status[:keys] = @chunk.keys
    end
    fields.each do |key|
      label = "#{key.to_s.gsub('_',' ')}:"
      value = status[key].inspect
      value += " #{units[key]}" if units[key]
      iputs "%-20s #{value}" % label
    end
  end

  def info_block_dump
    iputs "Info:"
    indent {iputs chunk.info}
  end

  def index_dump(block=nil,depth=1)
    unless block
      "chunk's index:"
    end
    indent do
      block||=@chunk.top_index_block
      iputs block.inspect
      if block.leaf?
        block.index_records.each do |key,ir|
          iputs " | "+ir.inspect
        end
      else
        block.index_records.each do |key,ir|
          index_dump(block.sub_index_block(ir),depth+1)
        end
      end
    end
  end

  def ls
    iputs "#{chunk.length} key(s) in chunk"
    chunk.each_key do |key|
      iputs key.inspect
    end
  end

  def get(key,field_names=nil)
    records=@chunk.get(key)
    if !records
      iputs "record for key #{key.inspect} does not exists"
    elsif field_names && field_names.length>0
      field_names.each do |field_name|
        iputs "#{field_name.inspect} => #{records[field_name].inspect}"
      end
    else
      iputs "field-names: "+records.keys.inspect
    end
  end

  def save_value(key,to_filename=nil)
    to_filename||=key
    value=chunk[key]
    if value
      iputs "Writing #{value.length} bytes to #{to_filename}"
      File.open(to_filename,"wb") {|f| f.write(value)}
    else
      iputs "key #{key.inspect} not found in #{chunk.filename.inspect}"
    end
  end
end

def show_usage(message=nil)
  puts <<ENDUSAGE
Monotable Tool

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
  info_block chunk_file

    Show the info_block_dump-block.

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
  opts = Trollop::options(args) do
    version v="Monotable Tool (MTT) v#{Monotable::VERSION} (c) Imikimi LLC (see LICENCE.TXT)"
    banner <<ENDBANNER
#{v}

Purpose: Inspect and extract data from the Monotable local_store.

Usage:

  #{__FILE__} [options] <chunk_files>

Options:
ENDBANNER
    opt :keys, "List chunk keys"
    opt :list, "List all keys in chunk"
    opt :dump, "Dump chunk's info"
    opt :get, "Get value for key in chunk", :type=>:string
  end

  mtts = args.collect {|file| MTTChunk.new(file)}.sort_by {|mtt| mtt.chunk.range_start}
  mtts.each do |mtt|
    mtt.indent "\n--------------------------------------------------------------\nchunk: #{mtt.chunk.filename}" do
      basics=true
      mtt.indent("list:"                     ) {basics=false;mtt.ls} if opts[:list]
      mtt.indent("get(#{opts[:get].inspect})") {basics=false;mtt.get(opts[:get])} if opts[:get]
      mtt.indent("dump info-block:"          ) {basics=false;mtt.info_block_dump} if opts[:dump]
      mtt.indent("dump index:"               ) {basics=false;mtt.index_dump} if opts[:dump]
      mtt.basics(:keys => opts[:keys]) if basics
    end
  end
end

tool(ARGV)
