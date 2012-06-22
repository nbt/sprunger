module ETL

  class TwitterJob
    # Synopsis:
    # Delayed::Job.enqueue TwitterJob.new(:load_friend_ids, twitter_id, cursor)
    # 
    # Delayed::Job will invoke TwitterJob.perform(), which will in
    # turn find an available TwitterProcessor and use it to process
    # the given method and arguments.  When the TwitterProcessor
    # method returns, it will become available for subsequent calls.

    attr_reader :method, :args

    def self.blather(msg)
      msg = "=== " + msg
      $stderr.puts(msg)
      Rails.logger.debug(msg)
    end

    def initialize(method, *args)
      @method = method
      @args = args
    end

    def perform
      self.class.blather("perform:#{self.method}(#{self.args.join(',')})")
      TwitterProcessor.with_processor do |processor| 
        self.class.blather("perform: TwitterProcessor[#{processor.id}].#{self.method}(#{self.args.join(',')})")
        c0 = Fact::Statement.count
        processor.send(self.method, *self.args)
        c1 = Fact::Statement.count
        self.class.blather("perform: TwitterProcessor[#{processor.id}].#{self.method}(#{self.args.join(',')}), added #{c1-c0} statements")
      end
    end

  end

end
