require 'update_or_insert'

module Fact

  class Statement < ActiveRecord::Base
    include UpdateOrInsert
    belongs_to :subject, :class_name => "Fact::Symbol"
    belongs_to :predicate, :class_name => "Fact::Symbol"
    belongs_to :target, :class_name => "Fact::Symbol"
    belongs_to :context, :class_name => "Fact::Symbol"

    validates :subject, :predicate, :target, :context, :presence => true

#     def self.create_tuple(subject, predicate, target, context)
#       self.where(:subject_id => Fact::Symbol.intern(subject).id,
#                  :predicate_id => Fact::Symbol.intern(predicate).id,
#                  :target_id => Fact::Symbol.intern(target).id,
#                  :context_id => Fact::Symbol.intern(context).id).first_or_create
#     end

    def self.tuple_relation(subject, predicate, target, context)
      self.where(:subject_id => Fact::Symbol.to_id(subject),
                 :predicate_id => Fact::Symbol.to_id(predicate),
                 :target_id => Fact::Symbol.to_id(target),
                 :context_id => Fact::Symbol.to_id(context))
    end

    def self.create_tuple(subject, predicate, target, context)
      tuple_relation(subject, predicate, target, context).first_or_create!
    end

    def to_s
      "#<#{self.class.name}##{self.id} '#{subject.try(:name)}' #{predicate.try(:name)} '#{target.try(:name)}' (ref: #{context.try(:name)})>"
    end

    def self.create_tuples(subjects, predicates, targets, contexts)
      tuples = build_tuples(subjects, predicates, targets, contexts)
      Fact::Statement.transaction do
        # Silliness: we've created full active records but use them
        # only for their fields.  This should be vectorized.
=begin
        tuples.each do |t| 
          self.where(:subject_id => t.subject_id,
                     :predicate_id => t.predicate_id,
                     :target_id => t.target_id,
                     :context_id => t.context_id).first_or_create!
        end
=end
        self.update_or_insert(tuples, :match => ["subject_id", "predicate_id", "target_id", "context_id"], :update => :none)
      end
    end

    # Return an array of unsaved Fact::Statement records, ready to be committed to 
    # the database.  Each argument (subjects, predicates, targets and contexts) may
    # be a string (a Fact::Symbol name), a Fact::Symbol record, or an integer (the
    # Fact::Symbol id), or a vector of same.
    #
    def self.build_tuples(subjects, predicates, targets, contexts)
      subjects, predicates, targets, contexts = coerce_columns_to_symbol_ids(subjects, predicates, targets, contexts)
      subjects.zip(predicates, targets, contexts).map {|s, p, t, c|
        Fact::Statement.new(:subject_id => s, :predicate_id => p, :target_id => t, :context_id => c)
      }
    end

    private

    def self.coerce_columns_to_symbol_ids(subjects, predicates, targets, contexts)
      columns = [Array(subjects), Array(predicates), Array(targets), Array(contexts)]
      max_length = columns.map {|el| el.size}.max
      columns.map {|column| coerce_column_to_symbol_ids(column, max_length)}
    end

    # Return a vector length elements long, where each element is a Fact::Symbol#id.
    def self.coerce_column_to_symbol_ids(column, length)
      case column.length
      when 1
        Array.new(length, Fact::Symbol.to_id(column.first))
      when length
        Fact::Symbol.transaction do
          column.map {|element| Fact::Symbol.to_id(element)}
        end
      else
        raise ArgumentError.new("column length mismatch")
      end
    end

  end

end
