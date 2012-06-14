require 'spec_helper'

describe Fact::Symbol do

  describe 'intern' do
  
    it 'on nil should raise error' do
      lambda { Fact::Symbol.intern(nil) }.should raise_error
    end

    it 'should create Fact::Symbol from string' do
      n = "fred"
      Fact::Symbol.intern(n).name.should == n
    end

    it 'should create Fact::Symbol from symbol' do
      n = :fred
      Fact::Symbol.intern(n).name.should == n.to_s
    end
      
    it 'should not create duplicate from string' do
      n = "fred"
      Fact::Symbol.intern(n)
      lambda { Fact::Symbol.intern(n) }.should_not change{Fact::Symbol.count}
    end

    it 'should not create duplicate from symbol' do
      n = :fred
      Fact::Symbol.intern(n)
      lambda { Fact::Symbol.intern(n) }.should_not change{Fact::Symbol.count}
    end

    it 'should return identity when given a Fact::Symbol' do
      a = Fact::Symbol.intern("fred")
      Fact::Symbol.intern(a).should == a
    end

  end

  describe 'to_id' do

    it 'should return fixnum for fixnum arg' do
      Fact::Symbol.to_id(333).should == 333
    end

    it 'should return id for existing symbol given a symbol' do
      symbol = Fact::Symbol.intern("fred")
      Fact::Symbol.to_id(symbol).should == symbol.id
    end

    it 'should return id for existing symbol given a string' do
      symbol = Fact::Symbol.intern("fred")
      Fact::Symbol.to_id("fred").should == symbol.id
    end

    it 'should return an id for a new symbol given a string' do
      Fact::Symbol.to_id("greg").should be_kind_of(Fixnum)
    end

  end


end
