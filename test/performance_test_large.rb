require "../lib/monotable/monotable.rb"
require "./mono_table_helper_methods.rb"

puts "MonoTableHelper.new.reset_temp_dir..."
MonoTableHelper.new.reset_temp_dir

puts "MonoTable::SoloDaemon.new..."
solo=MonoTable::SoloDaemon.new(:store_paths=>["tmp"],:max_chunk_size => 64*1024*1024, :max_journal_size => 128*1024*1024, :verbose => true, :async_compactions=>true)


def stats(mt)
  num_chunks=mt.chunks.length
  accounting_size=0
  mt.chunks.each {|k,v| accounting_size+=v.accounting_size}
  "#{mt.class}(accounting_size=#{accounting_size},chunks.length=#{num_chunks})"
end

def populate(mt,num)
puts "populate(#{num})"
  $last||=0
  fields={}
  num.times do |n|
    str=n.to_s+"|"
    fields[:data]=str*(1024/str.length)
    key="key#{'%010d'%$last}"
    $last+=1
    mt.set(key,fields)
    puts "writing #{n}/#{num} #{stats(mt)}" if (n%10000)==0
  end
  puts "last compact..."
  mt.compact #(:async => true)
  puts "CompactionManager.wait_for_compactors..."
  MiniEventMachine.wait_for_all_tasks
  puts "done writing #{num} records"
end

populate(solo,160*1024)
#populate(solo,1024)
