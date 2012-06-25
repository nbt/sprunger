# Tests (and design) for created_at and updated_at
# Design and test :batch_size
# Design and test multiple access / advisory locks
# Reinstate f_time
# Benchmark tests

require 'spec_helper'
require 'update_or_insert'

describe UpdateOrInsert do
  before(:all) do
    CreateTestRecord.up
  end
  after(:all) do
    CreateTestRecord.down
  end

  # ================================================================
  # test scaffolding and supporting models

  class CreateTestRecord < ActiveRecord::Migration
    # Create a model with all primary types.
    def self.up
      create_table(:test_records, :force => true) do |t|
        t.string :f_string
        t.text :f_text
        t.integer :f_integer
        t.float :f_float
        t.decimal :f_decimal
        t.datetime :f_datetime
        t.timestamp :f_timestamp
        # t.time :f_time
        t.date :f_date
        # t.binary :f_binary
        t.boolean :f_boolean
      end
    end
    def self.down
      drop_table :test_records
    end
  end

  class TestRecord < ActiveRecord::Base
    include UpdateOrInsert
  end

  def attribute_factory(i, options = {}) 
    day_0 = Time.zone.at(0).to_datetime + i
    {
      "f_string" => options.has_key?("f_string") ? options["f_string"] : sprintf("string %04d", i),
      "f_text" => options.has_key?("f_text") ? options["f_text"] : sprintf("text %04d", i),
      "f_integer" => options.has_key?("f_integer") ? options["f_integer"] : i,
      "f_float" => options.has_key?("f_float") ? options["f_float"] : i.to_f,
      "f_decimal" => options.has_key?("f_decimal") ? options["f_decimal"] : BigDecimal.new(i, 18),
      "f_datetime" => options.has_key?("f_datetime") ? options["f_datetime"] : day_0 + i,
      "f_timestamp" => options.has_key?("f_timestamp") ? options["f_timestamp"] : day_0 + i,
      # "f_time" => options.has_key?("f_time") ? options["f_time"] : (day_0 + i).to_time,
      "f_date" => options.has_key?("f_date") ? options["f_date"] : (day_0 + i).to_date,
      # "f_binary" => options.has_key?("f_binary") ? options["f_binary"] : [i].pack("N"),
      "f_boolean" => options.has_key?("f_boolean") ? options["f_boolean"] : i.even?
    }
  end

  def build_test_record(i, options = {})
    TestRecord.new(attribute_factory(i, options))
  end

  def build_test_records(n, options = {})
    n.times.map {|i| build_test_record(i, options) }
  end

  # return a hash of attributes, minus those listed in :except
  def attributes(r, options = {:except => ["id", "updated_at"]})
    (r.respond_to?(:attributes) ? r.attributes : r.to_hash).dup.delete_if {|k,v| options[:except].member?(k)}
  end

  def list_of_attributes(ar, options = {:except => ["id", "updated_at"]})
    ar.map {|e| attributes(e, options) }
  end

  # a1 and a2 are arrays of hash (assumed equal in length).
  # return a copy of a1, in for which each hash in a1 has
  # selected fields updated from the corresponding hash
  # in a2.
  # update_attributes([{:a => 1, :b =>2},{:a => 3, :b => 4}], 
  #                   [{:a => 100, :b =>200},{:a => 300, :b => 400}], 
  #                   [:b]) 
  # =>
  #                   [{:a => 1, :b =>200},{:a => 3, :b => 400}]
  def update_selected_attributes(a1, a2, fields)
    result = []
    a1.each_with_index do |h1, i|
      h2 = a2[i]
      h = h1.dup
      fields.each {|f| h[f] = h2[f] }
      result << h
    end
    result
  end

  # ================================================================
  # tests start here

  # ================================================================
  describe 'test factories and test helpers' do

    it 'should store and validate records' do
      n = 20
      
      attributes = n.times.map {|i| attribute_factory(i)}
      unsaved = attributes.map {|attrs| TestRecord.new(attrs)}
      
      # did we create the proper number of records?
      lambda { 
        attributes.each {|attrs| TestRecord.create!(attrs)}
      }.should change(TestRecord, :count).by(n)

      # do they have the proper values?
      list_of_attributes(TestRecord.all).should =~ list_of_attributes(unsaved)
    end
    
  end

  # ================================================================
  describe 'with no records' do
    describe 'with ActiveRecord adapter' do
      it 'should have no effect' do
        lambda { 
          TestRecord.update_or_insert([], :adapter => 'ActiveRecord')
        }.should_not raise_error
        lambda { 
          TestRecord.update_or_insert([], :adapter => 'ActiveRecord')
        }.should_not change(TestRecord, :count)
      end
    end                         # describe 'with ActiveRecord adapter' do
    describe 'with default adapter' do
      it 'should have no effect' do
        lambda { 
          TestRecord.update_or_insert([]) 
        }.should_not raise_error
        lambda { 
          TestRecord.update_or_insert([]) 
        }.should_not change(TestRecord, :count)
      end
    end                       # describe 'with default adapter' do
  end                         # describe 'with no records' do

  describe 'with blank :match' do
    before(:each) do
      # create overlapping records
      @incumbent_attributes = (0 .. 3).map {|i| attribute_factory(i)}
      @candidate_attributes = (2 .. 5).map {|i| attribute_factory(i, "f_string" => sprintf("xstring %04d", i))}

      @incumbents = @incumbent_attributes.map {|r| TestRecord.create!(r)}
      @candidates = @candidate_attributes.map {|r| TestRecord.new(r)}
    end

    describe 'with ActiveRecord adapter' do
      it 'should insert distinct records' do
        lambda {
          TestRecord.update_or_insert(@candidates, :match => nil, :adapter => 'ActiveRecord')
        }.should change(TestRecord, :count).by(@candidates.size)
        unchanged = @incumbent_attributes
        updated = []
        inserted = @candidate_attributes
        list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
      end
    end                         # describe 'with ActiveRecord adapter' do
    describe 'with default adapter' do
      it 'should insert distinct records' do
        lambda {
          TestRecord.update_or_insert(@candidates, :match => nil)
        }.should change(TestRecord, :count).by(@candidates.size)
        unchanged = @incumbent_attributes
        updated = []
        inserted = @candidate_attributes
        list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
      end
    end                       # describe 'with default adapter' do
  end                         # describe 'with blank :match' do

  describe 'with single field :match' do
    before(:each) do
      @match = "f_integer"
      # create overlapping records
      @incumbent_attributes = (0 .. 3).map {|i| attribute_factory(i)}
      @candidate_attributes = (2 .. 5).map {|i| attribute_factory(i, "f_string" => sprintf("xstring %04d", i))}

      @incumbents = @incumbent_attributes.map {|r| TestRecord.create!(r)}
      @candidates = @candidate_attributes.map {|r| TestRecord.new(r)}
    end

    describe 'with :update => :all' do
      describe 'with ActiveRecord adapter' do
        it 'should update existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :all, :adapter => 'ActiveRecord')
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = @candidate_attributes[0,2]
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should update existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :all)
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = @candidate_attributes[0,2]
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with default adapter' do
    end                         # describe 'with :update => :all' do

    describe 'with :update => :none' do
      describe 'with ActiveRecord adapter' do
        it 'should not update existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :none, :adapter => 'ActiveRecord')
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should NOT be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = @incumbent_attributes[2,2]
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should not update existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :none)
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should NOT be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = @incumbent_attributes[2,2]
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with default adapter' do
    end                         # describe 'with :update => :none' do

    describe 'with :update => :error' do
      describe 'with ActiveRecord adapter' do
        it 'should raise an error' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :error, :adapter => 'ActiveRecord')
          }.should raise_error(UpdateOrInsert::RecordNotUnique)
        end
      end                       # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should raise an error' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :error)
          }.should raise_error(UpdateOrInsert::RecordNotUnique)
        end
      end                       # describe 'with default adapter' do
    end                         # describe 'with :update => :error' do

    describe 'with :update => :f_string' do
      describe 'with ActiveRecord adapter' do
        it 'should update selected fields of existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => ["f_string"], :adapter => 'ActiveRecord')
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = update_selected_attributes(@incumbent_attributes[2,2],@candidate_attributes[0,2],["f_string"])
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                    # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should update selected fields of existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => ["f_string"])
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = update_selected_attributes(@incumbent_attributes[2,2],@candidate_attributes[0,2],["f_string"])
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with default adapter' do
    end                         # describe 'with :update => :f_string' do

    describe 'with :update => [:f_string, :f_text]' do
      describe 'with ActiveRecord adapter' do
        it 'should update selected fields of existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => ["f_string", "f_text"], :adapter => 'ActiveRecord')
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = update_selected_attributes(@incumbent_attributes[2,2],@candidate_attributes[0,2],["f_string", "f_text"])
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end         # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should update selected fields of existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => ["f_string", "f_text"])
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = update_selected_attributes(@incumbent_attributes[2,2],@candidate_attributes[0,2],["f_string", "f_text"])
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with default adapter' do
    end                        # describe 'with :update => [:f_string, :f_text]' do
  end                          # describe 'with single field :match' do

  describe 'with multi-field :match' do
    before(:each) do
      @match = ["f_integer", "f_decimal"]
      # create overlapping records
      @incumbent_attributes = (0 .. 3).map {|i| attribute_factory(i)}
      @candidate_attributes = (2 .. 5).map {|i| attribute_factory(i, "f_string" => sprintf("xstring %04d", i))}

      @incumbents = @incumbent_attributes.map {|r| TestRecord.create!(r)}
      @candidates = @candidate_attributes.map {|r| TestRecord.new(r)}
    end

    describe 'with :update => :all' do
      describe 'with ActiveRecord adapter' do
        it 'should update existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :all, :adapter => 'ActiveRecord')
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = @candidate_attributes[0,2]
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should update existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :all)
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = @candidate_attributes[0,2]
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with default adapter' do
    end                         # describe 'with :update => :all' do

    describe 'with :update => :none' do
      describe 'with ActiveRecord adapter' do
        it 'should not update existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :none, :adapter => 'ActiveRecord')
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should NOT be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = @incumbent_attributes[2,2]
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should not update existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :none)
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should NOT be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = @incumbent_attributes[2,2]
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with default adapter' do
    end                         # describe 'with :update => :none' do

    describe 'with :update => :error' do
      describe 'with ActiveRecord adapter' do
        it 'should raise an error' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :error, :adapter => 'ActiveRecord')
          }.should raise_error(UpdateOrInsert::RecordNotUnique)
        end
      end                       # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should raise an error' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => :error)
          }.should raise_error(UpdateOrInsert::RecordNotUnique)
        end
      end                       # describe 'with default adapter' do
    end                         # describe 'with :update => :error' do

    describe 'with :update => :f_string' do
      describe 'with ActiveRecord adapter' do
        it 'should update selected fields of existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => ["f_string"], :adapter => 'ActiveRecord')
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = update_selected_attributes(@incumbent_attributes[2,2],@candidate_attributes[0,2],["f_string"])
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                    # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should update selected fields of existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => ["f_string"])
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = update_selected_attributes(@incumbent_attributes[2,2],@candidate_attributes[0,2],["f_string"])
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end                       # describe 'with default adapter' do
    end                         # describe 'with :update => :f_string' do

    describe 'with :update => [:f_string, :f_text]' do
      describe 'with ActiveRecord adapter' do
        it 'should update selected fields of existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => ["f_string", "f_text"], :adapter => 'ActiveRecord')
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = update_selected_attributes(@incumbent_attributes[2,2],@candidate_attributes[0,2],["f_string", "f_text"])
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end         # describe 'with ActiveRecord adapter' do
      describe 'with default adapter' do
        it 'should update selected fields of existing records and insert new records' do
          lambda {
            TestRecord.update_or_insert(@candidates, :match => @match, :update => ["f_string", "f_text"])
          }.should change(TestRecord, :count).by(2)
          # last two incumbents should be updated by first two candidates
          unchanged = @incumbent_attributes[0,2]
          updated = update_selected_attributes(@incumbent_attributes[2,2],@candidate_attributes[0,2],["f_string", "f_text"])
          inserted = @candidate_attributes[2,2]
          list_of_attributes(TestRecord.all).should =~ list_of_attributes(unchanged + updated + inserted)
        end
      end         # describe 'with default adapter' do
    end           # describe 'with :update => [:f_string, :f_text]' do
  end             # describe 'with multi-field :match' do


  
end
