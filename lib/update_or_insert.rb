module UpdateOrInsert

  def self.included(base)
    base.extend(ClassMethods)
    base.send(:include, InstanceMethods)
  end

  module ClassMethods

    DEFAULT_OPTIONS = {
      :match => nil,
      :update => :all,
      :batch_size => nil,
      :db_adapter => nil,
    }

    # +update_or_insert+ efficiently writes an array of ActiveRecords
    # to the backing store.  If a record matches an entry already in
    # the backing store, that entry is updated; otherwise a new entry
    # is inserted.  Options specify which backing store fields are
    # used in determining a match and which fields are modified in an
    # update.
    #
    # +update_or_insert+ is generally faster than ActiveRecord#save
    # and its counterparts, gaining its speed by using bulk operations
    # tailored specifically to the backing store.
    #
    # Recognized options are:
    #
    # * +:match+: the name of a single field or an array of field names
    #             used in determining a match.  A blank +match+ implies
    #             'never match', so entries are never updated, only
    #             inserted.  Default: []
    #
    # * +:update+: Defines the action taken upon a match.  Can be one
    #              of :all (update all fields), :none (update none of
    #              the fields), :error (raise an error), or a list of
    #              field names to be updated from the new record.
    #              Default: :all
    #
    # * +:adapter+: Specifies which backing store adapter to use.  
    #               Default the adapter associated with the current
    #               ActiveRecord connection, but may be over-ridden.
    #               A value of 'ActiveRecord' disables backing-store
    #               specific commands and uses generic ActiveRecord
    #               methods.
    # * +:batch_size+: Specifies how many records will be processed
    #                  per transaction.  Default is adapter specific.
    # NOTES
    #
    # * +update_or_insert+ skips all validations and callbacks unless
    # +:adapter => 'ActiveRecord'+.
    #
    # * +:update => :all+ by default does NOT update any fields
    # designated as primary keys, nor any field named ":created_at".
    # This can be over-ridden by providing the full list of field
    # names, as in: :update => MyModel.column_names
    #
    # * Even when you don't specify +:update => :error+, the backing
    # store will raise an error if you attempt to insert a record that
    # results in a unique key violation.  In fact, if multiple
    # processes are accessing the backing store, unique key violations
    # are the only guaranteed means of detecting duplicates.
    #
    # TODO
    # * Accept ActiveRelations in lieu of an array of ActiveRecords
    #
    def update_or_insert(records, options = {})
      return if records.blank?
      options = DEFAULT_OPTIONS.merge(options)
      db_adapter = options.delete(:adapter) || ActiveRecord::Base.connection.adapter_name
      loader_class = self.const_get(db_adapter + "Loader")
      loader_class.new(self, options).update_or_insert(records)
    end
      
  end

  module InstanceMethods

  end

  class RecordNotUnique < StandardError ; end

  # ================================================================
  # common to all Loader classes

  class BaseLoader
    attr_reader :ar_class, :options

    def initialize(ar_class, options = {})
      @ar_class = ar_class
      @options = options
    end

  end

  # ================================================================
  # generic implementation using ActiveRecord methods.  Any backend
  # specific adaptor should produce the same results as this generic
  # implementation.
  
  class ActiveRecordLoader < BaseLoader

    def update_or_insert(records)
      matched_fields = Array(options[:match])
      updated_fields = if (options[:update] == :all)
                         ar_class.column_names - ["id", "created_at"]
                       elsif (options[:update] == :none) || (options[:update] == :error) || options[:update].blank?
                         []
                       elsif (options[:update].instance_of?(Array))
                         options[:update]
                       else
                         raise ArgumentError.new("unrecognized :update option #{options[:update]}")
                       end
      records.each do |record|
        matcher = Hash[matched_fields.map {|f| [f, record[f]]}]
        if matcher.present? && (relation = ar_class.where(matcher)).exists?
          # matching record exists: update if needed
          raise RecordNotUnique.new("record already exists: #{relation}") if options[:update] == :error
          updates = Hash[updated_fields.map {|f| [f, record[f]]}]
          relation.update_all(updates) unless updates.blank?
        else
          # matching record does not exist: create one
          ar_class.create!(record.attributes)
        end
      end
    end
    
  end

  # ================================================================
  # common to all sql-based backends

  class BaseSQLLoader < BaseLoader

    def update_or_insert(records)
      if options[:match].blank?
        do_insert_command(records)
      elsif (options[:update] == :all)
        do_update_insert_command(records, ar_class.column_names - ["id", "created_at"])
      elsif (options[:update].instance_of?(Array))
        do_update_insert_command(records, options[:update])
      elsif options[:update] == :none
        do_ignore_insert_command(records)
      elsif options[:update] == :error
        do_error_insert_command(records)
      else
        raise ArgumentError.new("unrecognized :update option #{options[:update]}")
      end
    end

    # insert without checking for duplicates
    def do_insert_command(records)
      execute_sql_command(compile_insert_command(records))
    end

    # update duplicates, insert non-duplicates
    def do_update_insert_command(records, column_names)
      execute_sql_command(compile_update_insert_command(records, column_names))
    end

    # ignore duplicates, insert non-duplicates
    def do_ignore_insert_command(records)
      execute_sql_command(compile_insert_if_absent_commands(records))
    end

    # raise error if any duplicates, insert otherwise
    def do_error_insert_command(records)
      resp = ActiveRecord::Base.connection.select_one(compile_duplicate_query(records))
      if (count = resp["count"].to_i) > 0
        raise (RecordNotUnique.new("found #{count} duplicate records"))
      else
        do_insert_commands(records)
      end
    end

    def execute_sql_command(cmd)
      ActiveRecord::Base.connection.execute(cmd)
    end

  end

  # ================================================================
  # Loader specifically for PostgreSQL

  class PostgreSQLLoader < BaseSQLLoader

    # insert w/o checking for updates
    def compile_insert_command(records)
      table_name = ar_class.table_name
      column_names = ar_class.column_names - [ar_class.primary_key]
      cmd =<<EOS
INSERT INTO #{table_name} ( #{generate_column_names(column_names)} ) VALUES
  #{generate_table_values(records, column_names)}
EOS
    end

    def compile_update_insert_command(records, fields_to_update)
      compile_update_if_present_commands(records, fields_to_update) + 
      ";" + 
      compile_insert_if_absent_commands(records)
    end

    # update records for which keys named in option[:match] match,
    # only updating fields names in fields_to_update:
    # 
    #     UPDATE test_records
    #        SET s = X.s
    #       FROM (      SELECT 3 AS i0, 3 AS i1, 'b3' AS s
    #             UNION SELECT 3, 103, 'b103') AS X
    #       WHERE X.i0 = test_records.i0
    #         AND X.i1 = test_records.i1
    #
    def compile_update_if_present_commands(records, fields_to_update)
      table_name = ar_class.table_name
      subtable_name = 'C'
      fields_to_match = Array(options[:match])
      fields_for_subtable = fields_to_match | fields_to_update
      s =<<EOS
      UPDATE #{table_name}
         SET #{generate_set_clause(fields_to_update, subtable_name)}
        FROM ( #{generate_immediate_table(records, fields_for_subtable)} ) AS #{subtable_name}
       WHERE #{generate_update_where_clause(fields_to_match, subtable_name)}
EOS
    end

    # insert records for which keys named in option[:match] do NOT
    # match.
    # 
    #     INSERT INTO test_records (i,s) 
    #          SELECT C.i, C.s
    #            FROM (      SELECT 1 AS i, 's101' AS s
    #                  UNION SELECT 4, 's104'
    #                 ) AS C
    # LEFT OUTER JOIN test_records
    #              ON test_records.i = C.i
    #           WHERE test_records.i IS NULL
    #
    def compile_insert_if_absent_commands(records)
      table_name = ar_class.table_name
      subtable_name = 'C'
      fields_to_match = Array(options[:match])
      subtable_column_names = ar_class.column_names - [ar_class.primary_key]
      s=<<EOS
    INSERT INTO #{table_name} (#{generate_column_names(subtable_column_names)})
         SELECT #{generate_column_names(subtable_column_names, subtable_name)}
           FROM (#{generate_immediate_table(records, subtable_column_names)} ) AS #{subtable_name}
LEFT OUTER JOIN #{table_name}
             ON #{generate_insert_on_clause(table_name, subtable_name, fields_to_match)}
          WHERE #{table_name}.id IS NULL
EOS
    end

    # returns the count of duplicate records
    def compile_duplicate_query(records)
      table_name = ar_class.table_name
      subtable_name = 'C'
      fields_to_match = Array(options[:match])
      s =<<EOS
      SELECT COUNT(*)
        FROM ( #{generate_immediate_table(records, fields_to_match)} ) AS #{subtable_name}
  INNER JOIN #{table_name}
          ON #{generate_insert_on_clause(table_name, subtable_name, fields_to_match)}
EOS
    end

    # ================

    def generate_table_values(records, column_names)
      records.map {|row| generate_row_values(row, column_names)}.join(",\n")
    end

    def generate_column_names(column_names)
      column_names.join(",")
    end

    def generate_row_values(record, column_names)
      "(" + column_names.map {|c| "#{ar_class.quote_value(record[c])}"}.join(",") + ")"
    end

    # without table name:
    # => f_float, f_integer
    # with table name:
    # => T.float, T.f_integer
    def generate_column_names(column_names, table_name = nil)
      column_names.map {|f| (table_name.blank? ? '' : "#{table_name}.") + f }.join(", ")
    end

    def generate_set_clause(fields_to_update, immediate_table_name)
      fields_to_update.map {|f| "#{f} = #{immediate_table_name}.#{f}"}.join(", ")
    end      

    # => table_name.f1 = C.f1 AND table_name.f2 = C.f2
    def generate_insert_on_clause(table_name, subtable_name, fields_to_match)
      fields_to_match.map {|f| "#{table_name}.#{f} = #{subtable_name}.#{f}"}.join(" AND ")
    end

    def generate_immediate_table(records, fields_for_immediate_table)
      columns_for_immediate_table = fields_for_immediate_table.map {|field| ar_class.columns.find {|c| c.name == field}}
      records.map {|record| generate_immediate_row(record, columns_for_immediate_table, record == records[0])}.join("\n")
    end

    def generate_immediate_row(record, columns_for_immediate_table, is_first)
      (is_first ? 'SELECT ' : 'UNION SELECT ') + 
        columns_for_immediate_table.map {|c| generate_immediate_value(record[c.name], c, is_first)}.join(", ")
    end

    def generate_immediate_value(value, column, is_first)
      "#{generate_typecast_value(value, column)}" +
        (is_first ? " AS #{column.name}" : "")
    end

    # => CAST ('2.3' AS double precision)
    # Note: we probably don't need it for all types
    def generate_typecast_value(value, column)
      # efficiency hack: Handle common types w/o calling super-slow
      # ar_class.quote_value
      case column.type
      when :string
        "'" + value + "'"
      when :integer
        value
      else
        "CAST (#{ar_class.quote_value(value)} AS #{column.sql_type})"
      end
    end

    def generate_update_where_clause(fields_to_match, subtable_name)
      table_name = ar_class.table_name
      fields_to_match.map {|f| "#{subtable_name}.#{f} = #{table_name}.#{f}"}.join(" AND ")
    end

  end



end
