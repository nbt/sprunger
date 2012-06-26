require 'spec_helper'
require 'update_or_insert'
require 'update_or_insert_helper'

# Spawn multiple processes that each attempt to update/insert
# items in a table.  Make sure that the correct number of 
# items are inserted.

describe UpdateOrInsert do
  before(:all) do
    UpdateOrInsertTest.create_tables
  end

  def self.concurrent_update_insert_test(n_processes, n_updates, n_inserts)
    describe "preloading #{n_updates} records" do
      before(:all) do
        UpdateOrInsertTest.create_update_records(n_updates)
      end
      it "should handle #{n_processes} processes, #{n_updates} updates, #{n_inserts} inserts" do
        prev_count = UpdateOrInsertTest.count
        lambda {
          n_processes.times.each { 
            Process.spawn("rails runner spec/lib/update_or_insert_task.rb --update #{n_updates} --insert #{n_inserts} --options ':match => \"s\", :update => [\"i\"], :logger => $stderr'")
          }
          Process.waitall.each {|pid, status| status.exitstatus.should == 0 }
        }.should change(UpdateOrInsertTest, :count).by(n_processes * n_inserts)
      end

      after(:all) do
        UpdateOrInsertTest.destroy_all
      end
    end
  end
  
  describe 'update with single process' do
    concurrent_update_insert_test(1, 2, 2)
    concurrent_update_insert_test(1, 4, 4)
    concurrent_update_insert_test(1, 8, 8)
    concurrent_update_insert_test(1, 16, 16)
    concurrent_update_insert_test(1, 32, 32)
    concurrent_update_insert_test(1, 64, 64)
    concurrent_update_insert_test(1, 128, 128)
    concurrent_update_insert_test(1, 256, 256)
    concurrent_update_insert_test(1, 512, 512)
    concurrent_update_insert_test(1, 1024, 1024)
    concurrent_update_insert_test(1, 2048, 2048)
    concurrent_update_insert_test(1, 4096, 4096)
    concurrent_update_insert_test(1, 8192, 8192)
    concurrent_update_insert_test(1, 16384, 16384)
  end

  describe 'update and insert in parallel' do
    concurrent_update_insert_test(2, 2, 2)
    concurrent_update_insert_test(2, 500, 500)
    concurrent_update_insert_test(10, 2, 2)
    concurrent_update_insert_test(10, 100, 100)
  end

  after(:all) do
    UpdateOrInsertTest.drop_tables
  end

end
