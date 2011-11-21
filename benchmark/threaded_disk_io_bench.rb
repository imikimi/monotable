require "benchmark"
Thread.abort_on_exception=true

class ThreadedDiskIOBench
  attr_accessor :temp_dir
  attr_accessor :file

  def initialize
    @filename="test.tmp"


    @write_size=1 * 1024 * 1024
    @total_written = 512 * 1024 * 1024
    @num_writes=@total_written / @write_size
    @str="a"*@write_size
    puts "write block size: #{@str.length}\n"
  end

  def open
    File.open(@filename,"wb") do |file|
      yield file
    end
  end

  def pip(c)
    $stdout.write(c);
    $stdout.flush
  end

  def sleepwritesleep(fsync=false)
    start_time=Time.now
    written=0
    open do |file|
      (@num_writes).times do
        pip "["
        file.write @str
        file.fsync if fsync
        pip "]"
        written+=@str.length
      end
    end
    delta=Time.now-start_time
    pip "\n(#{written/(1024*1024)} mB written - #{"%.2f"%((written/(delta*1024*1024)))} mB/s)"
    @sleepwritesleep_done=true
  rescue
    pip "!"
    raise
  end

  def dostuff
    a=0
    start_time=Time.now

    while !@sleepwritesleep_done
      pip "." if (a%2000000)==0
      a+=1
    end
    delta=Time.now-start_time
    pip "\n(a=#{a} - #{"%.2f"%(a/(delta*1000000))} +=1m/s)"
  end

  def sleepstuff
    while !@sleepwritesleep_done
      pip "."
      start_time=Time.now
      # dont work hard for .1 seconds
      sleep 0.1
    end
  end

  def bench
    testname=:thread_disk_io
    3.times do |time|
      puts "******************************************************************"
      puts "run #{time}"
      puts "******************************************************************"
    Benchmark.bm do |benchmarker|
      @sleepwritesleep_done=false
      benchmarker.report("\n%-40s"%"work-hard while writing w/o fsync") do
        [Thread.new {dostuff},Thread.new{sleepwritesleep}].each {|a| a.join}
        puts ""
      end
      @sleepwritesleep_done=false
      benchmarker.report("\n%-40s"%"work-hard while writing w fsync") do
        [Thread.new {dostuff},Thread.new{sleepwritesleep(true)}].each {|a| a.join}
        puts ""
      end
      @sleepwritesleep_done=false
      benchmarker.report("\n%-40s"%"don't work hard while writing w/o fsync") do
        [Thread.new {sleepstuff},Thread.new{sleepwritesleep}].each {|a| a.join}
        puts ""
      end
      @sleepwritesleep_done=false
      benchmarker.report("\n%-40s"%"don't work hard while writing w fsync") do
        [Thread.new {sleepstuff},Thread.new{sleepwritesleep(true)}].each {|a| a.join}
        puts ""
      end
    end
    end
  end
end

puts "This test proves that Ruby 1.9.x can do work while waiting for disk writes."
puts "The second thread can work hard or not and we still get the same write throughput."
ThreadedDiskIOBench.new.bench
