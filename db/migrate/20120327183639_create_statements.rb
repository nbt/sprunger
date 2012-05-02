class CreateStatements < ActiveRecord::Migration
  def change
    create_table :fact_statements do |t|
      t.references :subject
      t.references :predicate
      t.references :target
      t.references :context
    end
    add_index :fact_statements, [:subject_id, :predicate_id, :target_id], :name => :fact_statement_idx_on_fks, :unique => true
    # add more indices iff needed...
  end

end
