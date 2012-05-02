class CreateSymbols < ActiveRecord::Migration
  def change
    create_table :fact_symbols do |t|
      t.string :name
    end
    add_index :fact_symbols, :name
  end
end
