require File.join(File.dirname(__FILE__),"../lib/monotable/asi.rb")

describe Xbd::Asi do

  def test_asi(n)
    asi1=n.to_asi
    asi2=Xbd::Asi::ASI_INSTANCE.i_to_asi_c(n)
    raise "asi1!=asi2 (#{asi1.inspect}!=#{asi2.inspect}) n=#{n}" unless asi1==asi2
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

  it "should fail to convert 2^64 to an asi" do
    lambda {test_asi(2**64)}.should raise_error(RangeError)
  end
end
