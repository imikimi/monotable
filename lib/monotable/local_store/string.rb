# encoding: BINARY
=begin

N == max_string_length

Prev Guarantee: given a=b.binary_prev(N)
  * a < b
  * there exists no string c, c.length<=N, such that a < c < b

Next Guarantee: given a=b.binary_next(N)
  * a > b
  * there exists no string c, c.length<=N, such that a > c > b

To ensure these guarantees, we can consider:
  * all possible strings
  * with character values [0, 1, 2, ... 255]
  * and a maxium length of N
as strings
  * with exactly length == N
  * with character values [nil, 0, 1, 2, ... 255]
Where
  * Strings of length < N are logically padded with nil characters up to N.
Giving us Effectively
  * N-digit base 257 numbers

Then our logical algorithms become:

def next
  num = input_string.to_base257
  num+=1
  round_up_to_legal_binary_string(num)
end

def prev
  num = input_string.to_base257
  num-=1
  round_down_to_legal_binary_string(num)
end

base257 numbers are legal binary strings if the only "nil" digits are trailing digits.

def round_up_to_legal_binary_string(num)
  i = get_index_of_first_nil_digit(num)

  # keep everything to the left of the first nil the same and append the "0" digit
  # given our definition, the remaining N-i digits are "nil"
  num[0..i-1]<<"\x00"
end

def round_down_to_legal_binary_string(num)
  i = get_index_of_first_nil_digit(num)

  # just keep everything to the left of the first nil
  # given our definition, the remaining N-i digits are "nil"
  num[0..i-1]
end

The rounding methods can be better understood if we think in base-10. Let's
assume we want to convert any given number to the next/previous number that
only has trailing 0s. It has no 0s in between non-0 digits.

Ex: 12340000 is already OK.
Ex: 12300400 needs "rounding"
  12300400 rounded up is 12310000
  12300400 rounded down is 12300000

The implementations below are streamlined code that follows all the above rules
and therefor provide the guarantees at the top.

=end
module Monotable
  module StringBinaryEnumeration
    def binary_next(max_string_length=100)
#      raise ArgumentError.new("No next exists. Either max_string_length==0 or the input string is all \\xFFs up to max_string_length")
      return "" if max_string_length==0
      binself=self.to_binary
      if binself.length<max_string_length
        binself.clone<<"\x00"
      elsif (b=binself.byte(-1))<255
        # increment the last byte by one
        binself[0..-2]<<b+1
      else
        # carry the 1
        nxt=binself[0..-2].binary_next(max_string_length-1)
        nxt=self if nxt==binself[0..-2]
        nxt
      end
    end

    def binary_prev(max_string_length=100)
#      raise ArgumentError.new("No prev exists. The string is empty") if length==0
      binself=self.to_binary
      if binself=="" || binself=="\x00"
        ""
      elsif binself.byte(-1)==0
        binself[0..-2]
      else
        (binself[0..-2]<<(binself.byte(-1)-1))+("\xff"*(max_string_length-binself.length))
      end
    end
  end
end

class String
  include Monotable::StringBinaryEnumeration
end
