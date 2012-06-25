# This script is driven from the update_or_insert_concurrent_spec test
# file in order to exercise the update_or_insert method from
# concurrent tasks.  It reads --update, --insert and --options
# arguments from the command line to prebuild candidate records, then
# calls the #insert_or_update method with the prebuilt records and
# options.

require 'update_or_insert_helper'

class UpdateOrInsertTask

  attr_reader :update_count, :insert_count, :options

  def initialize(update_count, insert_count, options = {})
    @update_count = update_count
    @insert_count = insert_count
    @logger = options.delete(:logger)
    @options = options
  end

  def pid
    @pid ||= Process.pid
  end

  def log(msg)
    @logger.print(msg) if @logger
  end

  def updated_records
    @updated_records ||= UpdateOrInsertTest.build_update_records(pid, update_count)
  end

  def inserted_records
    @inserted_records = UpdateOrInsertTest.build_insert_records(pid, insert_count)
  end
  
  def run_task
    log("[#{pid}]...")
    UpdateOrInsertTest.update_or_insert(updated_records + inserted_records, options)
    log("[/#{pid}]")
  end

  # e.g. "--update 1234" => 1234
  def self.getarg(key, default)
    (i = ARGV.index(key)).nil? ? default : ARGV[i+1]
  end

  def self.run
    update_count = getarg("--update", 1000).to_i
    insert_count = getarg("--insert", 1000).to_i
    options = eval("{" + getarg("--options", ":match => 's', :update => ['i']") + "}")
    self.new(update_count, insert_count, options).run_task
  end

  # Just do it.
  run

end
