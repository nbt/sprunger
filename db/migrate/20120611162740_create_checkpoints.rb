class CreateCheckpoints < ActiveRecord::Migration
  def change
    create_table :checkpoints do |t|
      t.string :name
      t.string :state
      t.timestamps
    end
    add_index :checkpoints, :name, :unique => true
  end
end
