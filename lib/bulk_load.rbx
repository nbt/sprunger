module BulkLoad

  def self.include(base)
    base.extend(ClassMethods)
    base.send(:include, InstanceMethods)
  end

  # ================================================================
  module ClassMethods

    DEFAULT_OPTIONS = {
      :db_adapter => nil        # nil for current adapter
      :on_duplicate => :ignore, # :ignore | :overwrite | :error
      :keys => :all,            # which keys determine uniqueness?
      :slice_size => nil,       # # of records to process at one time
    }

    DEFAULT_SLICE_SIZE = 2000

    RECOGNIZED_DB_ADAPTERS = %w(ActiveRecord MySQL PostgreSQL SQLite)

    def bulk_load(records, options = {})
      options = DEFAULT_OPTIONS.merge(options)
      adapter_name = options.delete(:db_adapter) || ActiveRecord::Base.connection.adapter_name
      slice_size = options.delete(:slice_size) || DEFAULT_SLICE_SIZE
      on_duplicate = options.delete(:on_duplicate)

      loader = self.const_get(adapter_name + "Loader").new(self, options)
      records.each_slice(slice_size) do |slice|
        case on_duplicate
        when :ignore
          # on duplicate, do not update db
          loader.bulk_load_ignore(slice)
        when :overwrite
          # on duplicate, update db
          loader.bulk_load_overwrite(slice)
        when :error
          # on duplicate, raise an error
          loader.bulk_load_error(slice)
        else
          raise ArgumentError.new("unrecognized :on_duplicate option #{on_duplicate}")
        end
      end
    end

  # ================================================================
  module InstanceMethods
  end

  # ================================================================
  class RecordNotUnique < StandardError ; end

  # ================================================================
  class BaseLoader

    attr_reader :ar_class, :on_duplicate, :keys

    def initialize(ar_class, options)
      @ar_class = ar_class
      @on_duplicate = options[:on_duplicate]
      @keys = (options[:keys] == :all) ? nonprimary_column_names : Array[:keys]
    end

    def bulk_load_ignore(records)
      insert_unique_records(records)
    end

    def bulk_load_overwrite(records)
      overwrite_duplicate_records(records)
      insert_unique_records(records)
    end

    def bulk_load_error(records)
      error_on_duplicate_records(records)
      insert_unique_records(records)
    end
      
    # ================ may be subclassed

    def insert_unique_records(records)
      cmd = make_insert_unique_records_command(records)
      ActiveRecord::Base.connection.execute(cmd)
    end

    def overwrite_duplicate_records(records)
      cmd = make_overwrite_duplicate_records_command(records)
      ActiveRecord::Base.connection.execute(cmd)
    end

    def error_on_duplicate_records(records)
      cmd = make_count_duplicates_command(records)
      count = ActiveRecord::Base.connection.select_one(cmd)["count"].to_i
      raise(RecordNotUnique, "found #{count} duplicate records") if (count > 0)
    end

    # ================ immediate table

    # Create an ANSI-compliant SQL form directly from in-memory
    # records which, used as a sub-query, behaves like a table.
    # Example:
    #
    #       SELECT 2257 AS station_id, '2001-01-01' AS date, 22.5 AS temperature 
    # UNION SELECT 2257, '2001-01-02', 25.3
    # UNION SELECT 2257, '2001-01-03', 25.5
    #
    def immediate_table(records)
      columns = nonprimary_columns
      records.map {|r| immediate_row(columns, r, r == records[0]) }.join("\n")
    end

    def immediate_row(columns, row, is_first)
      if is_first
        "      SELECT " + columns.map {|c| immediate_column(row.read_attribute(c.name), c) + "AS " + c.name }.join(', ')
      else
        "UNION SELECT " + columns.map {|c| immediate_column(row.read_attribute(c.name), c) }.join(', ')
      end
    end

    # Emit value in a database-compatible format.  created_at and
    # updated_at fields get special treatment.  Subclasses may need to
    # augment this generic form.  For the definition of quote(), see:
    #
    # usr/lib/ruby/gems/1.9.1/gems/activerecord-3.0.5/lib/active_record/connection_adapters/abstract/quoting.rb
    def immediate_column(value, column)
      value = Time.zone.now if ((column.name == 'created_at') && value.nil?) || (column.name == 'updated_at')
      ActiveRecord::Base.connection.quote(value, column)
    end

    # ================ helpers

    def table_name
      @ar_class.table_name
    end

    def nonprimary_columns
      @ar_class.columns.reject {|c| c.primary}
    end

    def nonprimary_column_names
      nonprimary_columns.map {|c| c.name}
    end

  end

  # ================================================================
  # Support for PostgreSQL
  class PostgreSQLLoader < BaseLoader

    # PostgreSQL
    # UPDATE table 
    #    SET (column1, column2, ...) = (candidates.column1, candidates.column2 ...)
    #   FROM (candidates) AS candidates
    #  WHERE table.key1 = candidates.key1
    #    AND table.key2 = candidates.key2
    #    ...
    def make_overwrite_duplicate_records_command(records)
      sql_command = %{
UPDATE #{table_name}
   SET (#{mutable_column_names}) = (#{mutable_column_names('candidates')})
  FROM (#{immediate_table(records)}) AS candidates
 WHERE #{ self.keys.map {|k| "#{table_name}.#{k} = candidates.#{k}"}.join(' AND ') }
}
    end

    # PostgreSQL requires explicit casting of NULLs, timestamps and
    # strings: +x+ => +(CAST x AS <type>)+
    def immediate_column(value, column = nil)
      s = super(value, column)
      if column && (value.nil? || column.sql_type =~ /timestamp/ || column.sql_type =~ /character/)
        "CAST (#{s} AS #{column.sql_type})"
      else
        s
      end
    end

  end



end
