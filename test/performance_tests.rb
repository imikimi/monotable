require "benchmark"
require "../lib/monotable/monotable"
temp_dir=File.expand_path("tmp")
file=MonoTable::FileHandle.new(File.join(temp_dir,"/test.tmp"))

def write_min_record_asi(key,record,file)
  command="set"
  str=[
    command.length.to_asi,
    command,
    key.length.to_asi,
    key,
#    record.length.to_asi
    ]+record.keys.collect {|k| v=record[k];[k.length.to_asi,k,v.length.to_asi,v]}
  file.append MonoTable::Tools.to_asi_checksum_string(str.join) #.to_asi_string
  file.flush
end

def write_unchecked_record_asi(key,record,file)
  command="set"
  chunk="000000.mt_chunk"
  str=[
    command.length.to_asi,
    command,
    chunk.length.to_asi,
    chunk,
    key.length.to_asi,
    key,
#    record.length.to_asi
    ]+record.keys.collect {|k| v=record[k];[k.length.to_asi,k,v.length.to_asi,v]}
  file.append MonoTable::Tools.to_asi_checksum_string(str.join) #.to_asi_string
  file.flush
end

def write_monotable_entry(k,r,file)
  entry=MonoTable::Chunk.new
  entry.set(k,r)
  file.append entry.to_binary
  file.flush
end

def read_monotable_entry(file)
  MonoTable::Chunk.new(file)
end

def read_asi(file)
  checksum_string=MonoTable::Tools.read_asi_checksum_string_from_file(file)
  file = StringIO.new(checksum_string)
  command = file.read_asi_string
  key = file.read_asi_string
  fields={}
  while !file.eof?
    k=file.read_asi_string
    v=file.read_asi_string
    fields[k]=v
  end
  {:command=>command.intern,
  :key=>key,:fields=>fields}
end

def write_record_marshal(key,record,file)
  command="set"
  chunk="000000.mt_chunk"
  to_write={
    :cmd =>"set",
    :key => key,
    :rec => record,
    :chunk => chunk
    }
  str=Marshal.dump(to_write).to_asi_string
  file.append str
  fi5~le.flush
end

def read_marshal(file)
  m=file.read_asi_string
  Marshal.load(m)
end

def test(testname,benchmarker,file,records=500000)
  file.delete if file.exists?
  file.open_write(true)
  testname="%10s"%testname.to_s
  time=benchmarker.report("#{records}x: #{testname}") {(0..records).each do |a|
    key=a.to_s
    val={"value"=>key}
    yield key,val,file
  end}.real
  file.close
  puts "\tsize=#{file.size} time=#{(time*1000).to_i}ms mB/sec=#{"%.1f"%(file.size/(time*1024*1024))} records/sec=#{(records/time).to_i}"
end

def test_journal(testname,benchmarker,file,records=500000)
  file.delete if file.exists?
  file.close
  journal=MonoTable::Journal.new(file.filename)
  journal.journal_file.open_append
  testname="%10s"%testname.to_s
  time=benchmarker.report("#{records}x: #{testname}") {(0..records).each do |a|
    key=a.to_s
    val={"value"=>key}
    yield key,val,journal
  end}.real
  file.close
  puts "\tsize=#{file.size} time=#{(time*1000).to_i}ms mB/sec=#{"%.1f"%(file.size/(time*1024*1024))} records/sec=#{(records/time).to_i}"
end

def test_chunk(testname,benchmarker,file,records=500000)
  file.delete if file.exists?
  file.close
  chunk_file=MonoTable::DiskChunk.new(:filename=>"test_chunk")
  chunk_file.journal.journal_file.open_append
  testname="%10s"%testname.to_s
  time=benchmarker.report("#{records}x: #{testname}") {(0..records).each do |a|
    key=a.to_s
    val={"value"=>key}
    yield key,val,chunk_file
  end}.real
  file.close
  puts "\tsize=#{file.size} time=#{(time*1000).to_i}ms mB/sec=#{"%.1f"%(file.size/(time*1024*1024))} records/sec=#{(records/time).to_i}"
end

def test_local_store(testname,benchmarker,file,records=500000)
  file.delete if file.exists?
  file.close
  local_store=MonoTable::LocalStore.new(:store_paths=>[File.expand_path("tmp")])
  testname="%10s"%testname.to_s
  time=benchmarker.report("#{records}x: #{testname}") {(0..records).each do |a|
    key=a.to_s
    val={"value"=>key}
    yield key,val,local_store
  end}.real
  file.close
  puts "\tsize=#{file.size} time=#{(time*1000).to_i}ms mB/sec=#{"%.1f"%(file.size/(time*1024*1024))} records/sec=#{(records/time).to_i}"
end

def read_test(testname,benchmarker,file)
  file.open_read
  testname="%10s"%testname.to_s
  records=0
  time=benchmarker.report("read: #{testname}") do
    f=file.read_handle
    last_entry=nil
    while !f.eof
      last_entry=yield f
      records+=1
    end
    expected={:command=>:set, :key=>last_entry[:key], :fields=>{"value"=>last_entry[:key]}}
    raise "decode for first record failed. Decoded=#{last_entry.inspect} Expected=#{expected.inspect}" unless last_entry==expected
  end.real
  puts "\tsize=#{file.size} time=#{(time*1000).to_i}ms mB/sec=#{"%.1f"%(file.size/(time*1024*1024))} records/sec=#{(records/time).to_i}"
end

def journal_read_test(testname,benchmarker,file)
  file.close
  testname="%10s"%testname.to_s
  records=0
  journal=MonoTable::Journal.new(file.filename)
  time=benchmarker.report("read: #{testname}") do
    last_entry=nil
    journal.each_entry do |entry|
      last_entry=entry
      records+=1
    end

    expected={:command=>:set, :key=>last_entry[:key], :fields=>{"value"=>last_entry[:key]}, :chunk_file=>"0000000.mt_chunk"}
    raise "decode for first record failed. Decoded=#{last_entry.inspect} Expected=#{expected.inspect}" unless last_entry==expected
  end.real
  puts "\tsize=#{file.size} time=#{(time*1000).to_i}ms mB/sec=#{"%.1f"%(file.size/(time*1024*1024))} records/sec=#{(records/time).to_i}"
end

Benchmark.bm do |x|
  test(:min_possible,x,file,10000) {|key,val,file| write_min_record_asi(key,val,file);}
  test(:unchecked,x,file,10000) {|key,val,file| write_unchecked_record_asi(key,val,file);}
#  read_test(:asi,x,file) {|f| read_asi(f)}
  test_journal(:Journal,x,file,10000) {|key,val,journal| journal.set("0000000.mt_chunk",key,val);}
#  read_test(:JournalEntry,x,file)   {|f| MonoTable::Journal.read_entry(f)}
  journal_read_test(:Journal,x,file)
  test_chunk(:DiskChunk,x,file,10000) {|key,val,chunk_file| chunk_file.set(key,val);}
  test_local_store(:LocalStore,x,file,10000) {|key,val,ls| ls.set(key,val);}
#  test(:marshal,x,file,100000) {|key,val,file| write_record_marshal(key,val,file);}
#  read_test(:marshal,x,file) {|file| read_marshal(file);}
#  test(:entry,x,file,10000) {|key,val,file| write_monotable_entry(key,val,file);}
#  read_test(:entry,x,file) {|file| read_monotable_entry(file);}
end

=begin
  x.report("marshal,full,nowrite                       ") {(0..100000).each {|a| b=a.to_s;c=Marshal.dump({"value"=>a.to_s});[b.length.to_asi,b,c.length.to_asi,c].join}}
  file.delete if file.exists?
  x.report("marshal_full,external-append               ") {file.append {|f|(0..100000).each {|a| b=a.to_s;c=Marshal.dump({"value"=>a.to_s});f.write([b.length.to_asi,b,c.length.to_asi,c].join)}}}
  file.delete if file.exists?
  x.report("marshal_full,external-append,flush         ") {file.append {|f|(0..100000).each {|a| b=a.to_s;c=Marshal.dump({"value"=>a.to_s});f.write([b.length.to_asi,b,c.length.to_asi,c].join);f.flush}}}
  file.delete if file.exists?
  file.open_write
  x.report("marshal,full,internal-append,preopen       ") {(0..100000).each {|a| b=a.to_s;c=Marshal.dump({"value"=>a.to_s});file.append([b.length.to_asi,b,c.length.to_asi,c].join)}}
  file.delete if file.exists?
  file.open_write
  x.report("marshal,full,internal-append,preopen,flush ") {(0..100000).each {|a| b=a.to_s;c=Marshal.dump({"value"=>a.to_s});file.append([b.length.to_asi,b,c.length.to_asi,c].join);file.flush}}

  next
  file.delete if file.exists?
  file.open_write
  x.report(" 1x: entry,full,internal-append,preopen,flush   ") {(0..10000).each do |a|
    entry=MonoTable::Chunk.new
    entry.set(a.to_s,"value"=>a.to_s)
    file.append entry.to_binary
    file.flush
  end}

=end
