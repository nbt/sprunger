source 'https://rubygems.org'

gem 'rails', '3.2.1'

# Bundle edge Rails instead:
# gem 'rails', :git => 'git://github.com/rails/rails.git'

case ENV["RAILS_DB"]
when "SQLite"
  gem 'sqlite3'
  gem 'sqlite-ruby', :require => 'sqlite3'
when 'MySQL'
  gem 'mysql2'
  gem 'activerecord-mysql2-adapter'
when 'PostgreSQL'
  gem 'pg'   # , '0.10.0'
else
  gem 'pg'   # , '0.10.0'
end

gem 'thin'

gem 'delayed_job_active_record'
gem 'daemons'

# Gems used only for assets and not required
# in production environments by default.
group :assets do
  gem 'sass-rails',   '~> 3.2.3'
  gem 'coffee-rails', '~> 3.2.1'

  # See https://github.com/sstephenson/execjs#readme for more supported runtimes
  # gem 'therubyracer'

  gem 'uglifier', '>= 1.0.3'
end

gem 'jquery-rails'

gem 'mechanize'

gem 'twitter'

group :development do
  gem 'foreman'
  #  gem 'linecache19', '0.5.13'
  gem 'ruby-prof'
  #  gem 'ruby-debug19', :require => 'ruby-debug'    
end

group :test, :development do
  gem "rspec"
  gem "rspec-rails"
  gem "webrat"
end

# To use ActiveModel has_secure_password
# gem 'bcrypt-ruby', '~> 3.0.0'

# To use Jbuilder templates for JSON
# gem 'jbuilder'

# Use unicorn as the web server
# gem 'unicorn'

# Deploy with Capistrano
# gem 'capistrano'

# To use debugger
# gem 'ruby-debug19', :require => 'ruby-debug'
