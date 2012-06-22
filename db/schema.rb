# encoding: UTF-8
# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended to check this file into your version control system.

ActiveRecord::Schema.define(:version => 20120615214705) do

  create_table "checkpoints", :force => true do |t|
    t.string   "name"
    t.string   "state"
    t.datetime "created_at", :null => false
    t.datetime "updated_at", :null => false
  end

  add_index "checkpoints", ["name"], :name => "index_checkpoints_on_name", :unique => true

  create_table "delayed_jobs", :force => true do |t|
    t.integer  "priority",   :default => 0
    t.integer  "attempts",   :default => 0
    t.text     "handler"
    t.text     "last_error"
    t.datetime "run_at"
    t.datetime "locked_at"
    t.datetime "failed_at"
    t.string   "locked_by"
    t.string   "queue"
    t.datetime "created_at",                :null => false
    t.datetime "updated_at",                :null => false
  end

  add_index "delayed_jobs", ["priority", "run_at"], :name => "delayed_jobs_priority"

  create_table "etl_twitter_processors", :force => true do |t|
    t.string   "client_options"
    t.datetime "rate_limit_updated_at"
    t.datetime "rate_limit_will_reset_at"
    t.integer  "rate_limit_hit_limit"
    t.integer  "rate_limit_hit_count"
    t.integer  "requestor"
  end

  create_table "fact_statements", :force => true do |t|
    t.integer "subject_id"
    t.integer "predicate_id"
    t.integer "target_id"
    t.integer "context_id"
  end

  add_index "fact_statements", ["subject_id", "predicate_id", "target_id"], :name => "fact_statement_idx_on_fks", :unique => true

  create_table "fact_symbols", :force => true do |t|
    t.string "name"
  end

  add_index "fact_symbols", ["name"], :name => "index_fact_symbols_on_name", :unique => true

end
