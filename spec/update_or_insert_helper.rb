# Support for testing update_or_create functions

require 'update_or_insert'

class UpdateOrInsertTest < ActiveRecord::Base
  include UpdateOrInsert
  
  class CreateUpdateOrInsertTable < ActiveRecord::Migration
    # Create a model with a string and an integer
    def self.up
      create_table(:update_or_insert_tests, :force => true) do |t|
        t.integer :i
        t.string :s
      end
    end
    def self.down
      drop_table :update_or_insert_tests
    end
  end

  def self.create_tables
    CreateUpdateOrInsertTable.up
  end

  def self.drop_tables
    CreateUpdateOrInsertTable.down
  end

  # update_or_insert matches on the s field, updates the i field

  def self.build_update_records(pid, n)
    n.times.map {|i| UpdateOrInsertTest.new(:i => pid, :s => i.to_s) }
  end

  def self.build_insert_records(pid, n)
    n.times.map {|i| UpdateOrInsertTest.new(:i => pid, :s => "#{pid}:#{i}")}
  end
    
  def self.create_update_records(n)
    n.times.each {|i| UpdateOrInsertTest.create!(:i => 0, :s => i.to_s) }
  end

end
