class Checkpoint < ActiveRecord::Base
  serialize :state
end
