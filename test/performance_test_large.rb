require "../lib/monotable/monotable.rb"
require "./mono_table_helper_methods.rb"

MonoTableHelper.new.reset_temp_dir

solo=MonoTable::SoloDaemon.new(:store_paths=>["tmp"],:max_chunk_size => 16*1024*1024, :max_journal_size => 32*1024*1024, :verbose => true)
solo.get_chunk("").journal.hold_file_open


def stats(mt)
  num_chunks=mt.chunks.length
  accounting_size=0
  mt.chunks.each {|k,v| accounting_size+=v.accounting_size}
  "#{mt.class}(accounting_size=#{accounting_size},chunks.length=#{num_chunks})"
end

def populate(mt,num)
  $last||=0
  fields={}
  num.times do |n|
    str=n.to_s+"|"
    fields[:data]=str*(1024/str.length)
    key="key#{'%010d'%$last}"
    $last+=1
    mt.set(key,fields)
    puts "writing #{n}/#{num} #{stats(mt)}" if (n%1000)==0
  end
  puts "done writing #{num} records"
end

populate(solo,128*1024)
