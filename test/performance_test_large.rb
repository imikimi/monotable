require "../lib/monotable/monotable.rb"
solo=MonoTable::SoloDaemon.new("tmp")
solo.get_chunk("").journal.hold_file_open

def stats(mt)
  num_chunks=mt.chunks.length
  size=0
  mt.chunks.each {|k,v| size+=v.size}
  "#{mt.class}(size=#{size},chunks.length=#{num_chunks})"
end

def populate(mt,num,fields)
  $last||=0
  num.times do |n|
    key="key#{$last}"
    $last+=1
    mt.set(key,fields)
    puts "writing #{n}/#{num} #{stats(mt)}" if (n%1000)==0
  end
  puts "done writing #{num} records"
end

populate(solo,128*1024,{:data=>"a"*1024})
