module ETL

  require 'twitter'
  require 'with_retries'

  class TwitterProcessor < ActiveRecord::Base
    include WithRetries
    serialize :client_options

    RETRIED_NETWORK_ERRORS = [Twitter::Error::ServiceUnavailable, 
                              Faraday::Error::ConnectionFailed,
                              Errno::EADDRNOTAVAIL]

    IGNORED_NETWORK_ERRORS = [Twitter::Error::Unauthorized]
    IS_FOLLOWED_BY_SYMBOL = Fact::Symbol.intern("is followed by")
    HAS_FRIENDS_COUNT_SYMBOL = Fact::Symbol.intern("has friends count")
    HAS_FOLLOWERS_COUNT_SYMBOL = Fact::Symbol.intern("has followers count")
    HAS_SCREEN_NAME = Fact::Symbol.intern("has screen name")
    HAS_NAME = Fact::Symbol.intern("has name")
    SORTIE_SYMBOL = Fact::Symbol.intern("Friends of Celebrity sortie 1.1")

    def self.blather(msg)
      msg = "=== " + msg
      $stderr.puts(msg)
      Rails.logger.debug(msg)
    end

    # mini-benchmark: prints message with timing around
    # the given body.
    def self.with_logging(msg)
      t0 = Time.now
      $stderr.printf("=== %s...", msg)
      result = yield
      dt = Time.now - t0
      $stderr.printf("[%0.3f s]\n", dt)
      Rails.logger.debug(sprintf("=== %s...[%0.3f s]", msg, dt))
      result
    end

    # Find a twitter processor that not currently in use and has the
    # most favorable rate limiting.  Will block if no processors are
    # available.  +requestor+ is assumed to be a unique among all the
    # potential requestors.

    def self.with_processor(requestor = Process.pid)
      begin
        processor = self.reserve_processor(requestor)
        yield(processor)
      ensure
        self.release_processor(processor) if processor
      end
    end
      
    def self.reset_all
      self.all.each {|p| p.reset}
    end

    def reset
      self.rate_limit_updated_at = nil
      self.rate_limit_will_reset_at = nil
      self.rate_limit_hit_limit = nil
      self.rate_limit_hit_count = nil
      self.requestor = nil
      self.save!
    end

    def load_followers_of(twitter_id, cursor = -1)
      response = with_enhanced_calm do
        with_retries(:retry => RETRIED_NETWORK_ERRORS, :ignore => IGNORED_NETWORK_ERRORS) do
          self.class.with_logging("twitter_client.follower_ids(#{twitter_id}, :cursor => #{cursor})") {
            twitter_client.follower_ids(twitter_id.to_i, :cursor => cursor)
          }
        end
      end
      if response
        if (response.next_cursor != 0)
          # load the next batch of followers
          Delayed::Job.enqueue TwitterJob.new(:load_followers_of, twitter_id, response.next_cursor)
        end
        follower_ids = response.ids.map(&:to_s)
        ActiveRecord::Base.silence do
          self.class.with_logging("Fact::Statement.create_tuples for #{follower_ids.count} followers") {
            Fact::Statement.create_tuples(twitter_id.to_s, IS_FOLLOWED_BY_SYMBOL, follower_ids, SORTIE_SYMBOL)
          }
        end
      end
    end

    def load_friends_of(follower_id, cursor = -1)
      response = with_enhanced_calm do
        with_retries(:retry => RETRIED_NETWORK_ERRORS, :ignore => IGNORED_NETWORK_ERRORS) do
          self.class.with_logging("twitter_client.friend_ids(#{follower_id}, :cursor => #{cursor})") {
            twitter_client.friend_ids(follower_id.to_i, :cursor => cursor)
          }
        end
      end
      if response
        if (response.next_cursor != 0)
          # load the next batch of friends
          Delayed::Job.enqueue TwitterJob.new(:load_friends_of, follower_id, response.next_cursor)
        end
        friend_ids = response.ids.map(&:to_s)
        ActiveRecord::Base.silence do
          self.class.with_logging("Fact::Statement.create_tuples for #{friend_ids.count} friends") {
            Fact::Statement.create_tuples(friend_ids, IS_FOLLOWED_BY_SYMBOL, follower_id.to_s, SORTIE_SYMBOL)
          }
        end
      end
    end

    def load_user_info(twitter_id)
      response = with_enhanced_calm do
        with_retries(:retry => RETRIED_NETWORK_ERRORS, :ignore => IGNORED_NETWORK_ERRORS) do
          self.class.with_logging("twitter_client.user(#{twitter_id})") {
            twitter_client.user(twitter_id.to_i)
          }
        end
      end
      if response
        ActiveRecord::Base.silence do
          self.class.with_logging("Fact::Statement.create_tuples for user #{response["screen_name"]} (followers_count = #{response["followers_count"]}") {
            Fact::Statement.create_tuple(twitter_id.to_s, HAS_NAME_SYMBOL, response["name"].to_s, SORTIE_SYMBOL)
            Fact::Statement.create_tuple(twitter_id.to_s, HAS_SCREEN_NAME_SYMBOL, response["screen_name"].to_s, SORTIE_SYMBOL)
            Fact::Statement.create_tuple(twitter_id.to_s, HAS_FRIENDS_COUNT_SYMBOL, response["friends_count"].to_s, SORTIE_SYMBOL)
            Fact::Statement.create_tuple(twitter_id.to_s, HAS_FOLLOWERS_COUNT_SYMBOL, response["followers_count"].to_s, SORTIE_SYMBOL)
          }
        end
      end
    end

    # Return the time at which we should hit the twitter server with
    # a query, following a linear ramp from the time we update the
    # limit status to twitter's reset time.
    def ideal_hit_time
      update_rate_limit_state_if_needed
      # ideal_hit_time ramps from updated_at to will_reset_at as hit_count
      # goes from 0 to hit_limit.
      linear_interpolate(self.rate_limit_hit_count, 
                         0, 
                         self.rate_limit_hit_limit, 
                         self.rate_limit_updated_at, 
                         self.rate_limit_will_reset_at)
    end
    
    # ================
    private

    def twitter_client
      @twitter_client ||= Twitter::Client.new(self.client_options)
    end

    # Find the TwitterProcessor with the earliest hit time that is
    # not not reserved, mark it as reserved and return it.  Blocks
    # with a sleep(3.0) loop until one comes available.
    def self.reserve_processor(requestor = Process.pid)
      while (true) do
        # TODO: Note that self#ideal_hit_time is NOT thread safe
        pairs = TwitterProcessor.all.map {|p| [p, p.ideal_hit_time]}
        processors = pairs.sort {|a, b| a[1] <=> b[1] }.map {|pair| pair[0]}
        available = processors.find {|p| p.requestor.nil?}
        TwitterProcessor.transaction {
          if available
            available.update_column(:requestor, requestor)
            available.reload
          end
        }
        if (available && available.requestor == requestor)
          return available 
        end
        self.blather("waiting for an available processor")
        sleep(3.0)
      end
    end

    def self.release_processor(p)
      TwitterProcessor.transaction {
        p.update_column(:requestor, nil)
      }
    end

    # ================
    # Rate limiting

    def rate_limit_status
      with_retries(:retry => RETRIED_NETWORK_ERRORS) {
        twitter_client.rate_limit_status
      }
    end

    def with_enhanced_calm
      sleep_until(ideal_hit_time)
      self.increment!(:rate_limit_hit_count)
      yield
    end

    # Return immediately if t is in the past, otherwise sleep
    # for an interval rounded up to the nearest second.
    def sleep_until(time)
      dt = (time - Time.zone.now).ceil
      self.class.blather("TwitterProcessor[#{self.id}].sleep_until: dt = #{dt}, hit_count = #{self.rate_limit_hit_count}, hits_remaining = #{self.rate_limit_hit_limit - self.rate_limit_hit_count}, seconds remaining = #{self.rate_limit_will_reset_at - Time.zone.now}")
      sleep(dt) if dt > 0.0
    end

    # Return only after rate_limit_hit_limit > 0, sleeping until the
    # end of the rate_limit period if needed.
    def update_rate_limit_state_if_needed
      self.rate_limit_hit_limit ||= 0
      self.rate_limit_hit_count ||= 0
      while (((self.rate_limit_hit_limit - self.rate_limit_hit_count) <= 0) || self.rate_limit_will_reset_at.nil? || (self.rate_limit_will_reset_at <= Time.zone.now)) do
        update_rate_limit_state
        sleep_until(self.rate_limit_will_reset_at) if (self.rate_limit_hit_limit == 0)
      end
    end

    def update_rate_limit_state
      status = rate_limit_status
      self.class.blather("TwitterProcessor[#{self.id}].update_rate_limit: status = #{status.inspect}")
      self.rate_limit_updated_at = Time.zone.now
      self.rate_limit_will_reset_at = status.reset_time
      self.rate_limit_hit_limit = status.remaining_hits
      self.rate_limit_hit_count = 0
      self.save!
    end
    
    def linear_interpolate(x, x0, x1, y0, y1)
      return y0 + (x - x0) * (y1 - y0) / (x1 - x0)
    end
    
  end
  
end
