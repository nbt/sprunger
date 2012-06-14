require 'spec_helper'
require 'with_retries'

describe WithRetries do

  class MyError1 < StandardError ; end
  class MyError2 < StandardError ; end

  it 'should complete simple body without error' do
    a = WithRetries::with_retries do
      5
    end
    a.should == 5
  end

  it 'should raise uncaught error' do
    lambda {
      WithRetries::with_retries do
        raise MyError1.new("woof")
      end
    }.should raise_error(MyError1)
  end

  it 'should ignore error as singleton' do
    lambda {
      WithRetries::with_retries(:ignore => MyError1) do
        raise MyError1.new("woof")
      end
    }.should_not raise_error
  end

  it 'should ignore error in list' do
    lambda {
      WithRetries::with_retries(:ignore => [MyError1, MyError2]) do
        raise MyError2.new("woof")
      end
    }.should_not raise_error
  end

  it 'should retry error as singleton' do
    max_retries = 2
    attempts = 0
    lambda {
      WithRetries::with_retries(:retry => MyError1, :delay_exponent => 0.0, :max_retries => max_retries) do
        attempts += 1
        raise MyError1.new("woof")
      end
    }.should raise_error
    attempts.should == max_retries + 1
  end

  it 'should retry error in list' do
    max_retries = 2
    attempts = 0
    lambda {
      WithRetries::with_retries(:retry => [MyError1, MyError2], :delay_exponent => 0.0, :max_retries => max_retries) do
        attempts += 1
        raise MyError2.new("woof")
      end
    }.should raise_error
    attempts.should == max_retries + 1
  end

end
