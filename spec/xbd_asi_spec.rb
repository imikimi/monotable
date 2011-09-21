require File.join(File.dirname(__FILE__),"xbd_test_helper")

module Xbd
describe Asi do

  def test_asi(n)
    asi1=n.to_asi
    asi2=Asi::ASI_INSTANCE.i_to_asi_c(n)
    raise "(asi1=n.to_asi)!=asi2 (#{asi1.inspect}!=#{asi2.inspect}) n=#{n} asi1.encoding=#{asi1.encoding} asi2.encoding=#{asi2.encoding}" unless asi1==asi2
    asi1.read_asi[0].should == n
  end

  it "should be possible to convert all powers of two up to 2^64-1" do
    v=0
    65.times do
      n=2**v-1
      test_asi(n)
      v+=1
    end
  end

  if $ruby_inline
  it "should fail to convert 2^64 to an asi" do
    lambda {test_asi(2**64)}.should raise_error(RangeError)
  end
  end

  it "Fixnum > String > Fixnum" do
    test_edges do |num|
      num.to_asi.from_asi.should==num
    end
  end

  it "to_asi_string and StringIO" do
    test_string="foo"
    asi_foo=test_string.to_asi_string
    StringIO.new(asi_foo).read_asi_string.should==test_string
  end

  it "to_asi and StringIO" do
    test_edges do |num|
      asi=num.to_asi
      StringIO.new(asi).read_asi.should==num
    end
  end

  it "to_asi_string and String" do
    test_string="foo"
    asi_foo=test_string.to_asi_string
    asi_foo.read_asi_string.should== [test_string,asi_foo.length]
  end

  it "asi_length" do
    test_edges do |n,c|
      n.asi_length.should==c
    end
  end

  def test_edges
    yield 0,1

    (1..10).each do |c|
      n=(1<<(7*c))
      n_1=n-1
      yield n_1,c
      yield n,c+1
    end
  end
end
end
