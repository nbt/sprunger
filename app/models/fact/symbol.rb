module Fact

  class Symbol < ActiveRecord::Base
    validates :name, :presence => true
    
    # Create the symbol if it doesn't exist.
    def self.intern(name)
      name.kind_of?(self) ? name : (r = where(:name => name.to_s)).first_or_create!
    rescue ActiveRecord::RecordNotUnique
      r.first
    end

    def self.to_id(obj)
      obj.instance_of?(Fixnum) ? obj : self.intern(obj).id
    end

  end

end
