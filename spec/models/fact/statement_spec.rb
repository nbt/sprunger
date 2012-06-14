require 'spec_helper'

describe Fact::Symbol do

  describe 'create_tuple' do
    
    it 'should create tuple from symbol names' do
      fact = Fact::Statement.create_tuple("s", "p", "t", "c")
      fact.subject.name.should == 's'
      fact.predicate.name.should == 'p'
      fact.target.name.should == 't'
      fact.context.name.should == 'c'
    end

    it 'should create tuple from symbols' do
      s = Fact::Symbol.intern('s')
      p = Fact::Symbol.intern('p')
      t = Fact::Symbol.intern('t')
      c = Fact::Symbol.intern('c')
      fact = Fact::Statement.create_tuple(s, p, t, c)
      fact.subject.name.should == 's'
      fact.predicate.name.should == 'p'
      fact.target.name.should == 't'
      fact.context.name.should == 'c'
    end

    it 'should create tuple from ids' do
      sid = Fact::Symbol.to_id('s')
      pid = Fact::Symbol.to_id('p')
      tid = Fact::Symbol.to_id('t')
      cid = Fact::Symbol.to_id('c')
      fact = Fact::Statement.create_tuple(sid, pid, tid, cid)
      fact.subject.name.should == 's'
      fact.predicate.name.should == 'p'
      fact.target.name.should == 't'
      fact.context.name.should == 'c'
    end

  end

  describe 'create_tuples' do

    it 'should create tuples from symbol names' do
      lambda { Fact::Statement.create_tuples(["s0", "s1"], ["p0", "p1"], ["t0", "t1"], ["c0", "c1"]) }.should change(Fact::Statement, :count).by(2)
      a = Fact::Statement.first
      a.subject.name.should == 's0'
      a.predicate.name.should == 'p0'
      a.target.name.should == 't0'
      a.context.name.should == 'c0'
    end

    it 'should create tuples from symbols' do
      s0, s1, p0, p1, t0, t1, c0, c1 = ["s0", "s1", "p0", "p1", "t0", "t1", "c0", "c1"].map {|x| Fact::Symbol.intern(x)}
      lambda { Fact::Statement.create_tuples([s0, s1], [p0, p1], [t0, t1], [c0, c1]) }.should change(Fact::Statement, :count).by(2)
      a = Fact::Statement.first
      a.subject.name.should == 's0'
      a.predicate.name.should == 'p0'
      a.target.name.should == 't0'
      a.context.name.should == 'c0'
    end

    it 'should create tuples from symbol ids' do
      s0, s1, p0, p1, t0, t1, c0, c1 = ["s0", "s1", "p0", "p1", "t0", "t1", "c0", "c1"].map {|x| Fact::Symbol.to_id(x)}
      lambda { Fact::Statement.create_tuples([s0, s1], [p0, p1], [t0, t1], [c0, c1]) }.should change(Fact::Statement, :count).by(2)
      a = Fact::Statement.first
      a.subject.name.should == 's0'
      a.predicate.name.should == 'p0'
      a.target.name.should == 't0'
      a.context.name.should == 'c0'
    end

    it 'should allow singleton subject arg' do
      s, p0, p1, t0, t1, c0, c1 = ["s", "p0", "p1", "t0", "t1", "c0", "c1"].map {|x| Fact::Symbol.intern(x)}
      lambda { Fact::Statement.create_tuples(s, [p0, p1], [t0, t1], [c0, c1]) }.should change(Fact::Statement, :count).by(2)
      a = Fact::Statement.first
      a.subject.name.should == 's'
      a.predicate.name.should == 'p0'
      a.target.name.should == 't0'
      a.context.name.should == 'c0'
      a = Fact::Statement.last
      a.subject.name.should == 's'
      a.predicate.name.should == 'p1'
      a.target.name.should == 't1'
      a.context.name.should == 'c1'
    end

  end

end
