module ETL

  Dir[Rails.root.join("app/models/etl/**/*.rb").to_s].each {|f| require f}

end
