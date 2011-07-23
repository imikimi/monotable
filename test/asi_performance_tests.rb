require "rubygems"
#require "inline"
require "benchmark"
require "../lib/monotable/asi.rb"

def bench(benchmarker,val)
  asi=Xbd::Asi.new
  num_loops = 1000000
  puts "testing #{num_loops} times: #{val}.to_asi == #{val.to_asi.inspect}"
  num_loops /= 10 # each loop does 10 inside
  time=benchmarker.report("asi-ruby           ") do
    num_loops.times do
      Xbd::Asi.i_to_asi_ruby(val)
      Xbd::Asi.i_to_asi_ruby(val)
      Xbd::Asi.i_to_asi_ruby(val)
      Xbd::Asi.i_to_asi_ruby(val)
      Xbd::Asi.i_to_asi_ruby(val)
      Xbd::Asi.i_to_asi_ruby(val)
      Xbd::Asi.i_to_asi_ruby(val)
      Xbd::Asi.i_to_asi_ruby(val)
      Xbd::Asi.i_to_asi_ruby(val)
      Xbd::Asi.i_to_asi_ruby(val)
    end
  end
  time=benchmarker.report("asi-c (1-dispatch) ") do
    num_loops.times do
      asi.i_to_asi_c(val)
      asi.i_to_asi_c(val)
      asi.i_to_asi_c(val)
      asi.i_to_asi_c(val)
      asi.i_to_asi_c(val)
      asi.i_to_asi_c(val)
      asi.i_to_asi_c(val)
      asi.i_to_asi_c(val)
      asi.i_to_asi_c(val)
      asi.i_to_asi_c(val)
    end
  end
  time=benchmarker.report("asi-c (2-dispatch) ") do
    num_loops.times do
      Xbd::Asi::i_to_asi2(val)
      Xbd::Asi::i_to_asi2(val)
      Xbd::Asi::i_to_asi2(val)
      Xbd::Asi::i_to_asi2(val)
      Xbd::Asi::i_to_asi2(val)
      Xbd::Asi::i_to_asi2(val)
      Xbd::Asi::i_to_asi2(val)
      Xbd::Asi::i_to_asi2(val)
      Xbd::Asi::i_to_asi2(val)
      Xbd::Asi::i_to_asi2(val)
    end
  end
end

Benchmark.bm do |benchmarker|
  bench benchmarker,0
  bench benchmarker,127
  bench benchmarker,2**7
  bench benchmarker,2**14
  bench benchmarker,2**21
  bench benchmarker,2**28
  bench benchmarker,(2**64)-1
end

