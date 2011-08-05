#!/usr/local/bin/ruby
require File.join(File.dirname(__FILE__),"..","lib","monotable","monotable")

def show_usage(message=nil)
  puts <<ENDUSAGE
MonoTable Compactor

Used internally by the deamon to do out-of-processes jounral compaction.

Usage: #{__FILE__} journal_file

ENDUSAGE
  puts message if message
  exit 1
end

def required_args(args,count,args_info)
  if args.length-1 < count
    show_usage("#{count} args expect: #{args_info}")
  end
end

def compact(journal_file)
  unless journal_file
    show_usage
  end
  unless File.exists?(journal_file)
    $stderr.puts "error: journal file #{journal_file.inspect} does not exist."
    return 2
  end
  begin
    Journal.compact_phase_1(journal_file)
    puts "SUCCESS"
  rescue Exception => e
    $stderr.puts "error: #{e.inspect}"
    $stderr.puts "\t#{e.backtrace.join("\n\t")}"
    return 3
  end
end

exit compact(ARGV[0])
