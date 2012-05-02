module Fact

  class Statement < ActiveRecord::Base
    belongs_to :subject, :class_name => "Fact::Symbol"
    belongs_to :predicate, :class_name => "Fact::Symbol"
    belongs_to :target, :class_name => "Fact::Symbol"
    belongs_to :context, :class_name => "Fact::Symbol"

    validates :subject, :predicate, :target, :context, :presence => true

    def self.create_tuple(subject, predicate, target, context)
      self.where(:subject_id => Fact::Symbol.intern(subject).id,
                 :predicate_id => Fact::Symbol.intern(predicate).id,
                 :target_id => Fact::Symbol.intern(target).id,
                 :context_id => Fact::Symbol.intern(context).id).first_or_create
    end
                             
    def to_s
      "#<#{self.class.name} '#{subject.name}' #{predicate.name} '#{target.name}' (ref: #{context.name})>"
    end

  end

end
