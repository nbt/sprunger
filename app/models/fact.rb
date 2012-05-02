module Fact

  def self.table_name_prefix
    'fact_'
  end

  Dir[Rails.root.join("app/models/fact/**/*.rb").to_s].each {|f| require f}

end
