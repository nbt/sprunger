class CreateTwitterProcessors < ActiveRecord::Migration
  def change
    create_table :etl_twitter_processors do |t|
      t.string :client_options
      t.timestamp :rate_limit_updated_at
      t.timestamp :rate_limit_will_reset_at
      t.integer :rate_limit_hit_limit
      t.integer :rate_limit_hit_count
      t.integer :requestor, :default => nil
    end
  end
end
