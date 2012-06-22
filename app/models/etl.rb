module ETL

  def self.table_name_prefix
    'etl_'
  end

  Dir[Rails.root.join("app/models/etl/**/*.rb").to_s].each {|f| require f}

end
