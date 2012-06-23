class FullIndexOnStatements < ActiveRecord::Migration
  def up
    remove_index :fact_statements, :name => :fact_statement_idx_on_fks
    add_index :fact_statements, [:subject_id, :predicate_id, :target_id, :context_id], :name => :fact_statement_idx_on_fks, :unique => true
  end

  def down
    remove_index :fact_statements, :name => :fact_statement_idx_on_fks
    add_index :fact_statements, [:subject_id, :predicate_id, :target_id], :name => :fact_statement_idx_on_fks, :unique => true
  end
end
