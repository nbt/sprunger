# Given Twitter entity x0, find other Twitter entities 
# that share a large community of followers.
#
# Step 1: Convert x0's twitter name to twitter id
# Step 2: find Y = all twitter ids that follow x0.
#         Create 'x0 is followed by ym' db entry for each
# Step 3: find X = all twitter ids followed by Y
#         Create 'xn is followed by ym' db entry for each
# Step 4: find the xn with largest counts

# Todo:
# Don't reset rate_limit at each rescued error.
# Reset rate_limit when entering top-level function
# Identify which errors should be retried after a delay
# Incorporate bulk loading

module ETL

  # Load things into Fact table via Twitter API.
  # see: 
  # https://dev.twitter.com/docs
  # https://dev.twitter.com/docs/using-search
  
  # SYNOPSIS:
  # optionally provide custom credentials
  # tl = ETL::TwitterLoader.new 
  # tl.find_shared_followers("SwatchUS")

  require 'twitter'
  require 'with_checkpoint'
  require 'with_retries'

  class TwitterLoader < ETL::Base
    include WithCheckpoint
    include WithRetries

    attr_reader :twitter_client, :target_id, :target_id_symbol, :rate_limiter

    IS_FOLLOWED_BY_SYMBOL = Fact::Symbol.intern("is followed by")
    SORTIE_SYMBOL = Fact::Symbol.intern("Friends of Celebrity sortie 1.0")

    DEFAULT_AUTHORIZATION = {
      :oauth_token => "7477802-zGgEK0FLcHrXNZYPfvOQmha31k8cUzpP6jh1Zpst74",
      :oauth_token_secret => "JMncmgj6gHnllGfEcKsS7oIlikiq8dElEcCxIHWnmA",
      :consumer_key => "xwiCZr2DoCkbC1RFBqpdA",
      :consumer_secret => "evVgk6XTn6XnUl3dHyi3yvYKnLTyksOfggCokmLU8fM"
    }
    
    # Authorization is an optons hash passed to Twitter::Client.new
    def initialize(authorization = DEFAULT_AUTHORIZATION)
      @twitter_client = ::Twitter::Client.new(authorization)
      @rate_limiter = RateLimiter.new(@twitter_client)
    end

    def find_shared_followers(name)
      begin
        rate_limiter.reset
        # This used to be :step_1, but we always need to set up @target_id
        convert_target_name_to_id(name) unless @target_id
        with_checkpoint("TwitterLoader##{__method__}(#{name})", :step_2) do |checkpoint|
          $stderr.puts("=== find_shared_followers(0), state = #{checkpoint.state}")
          case checkpoint.state
          when :step_2
            load_followers_of_target
            $stderr.puts("=== find_shared_followers(2), state = #{checkpoint.state}")
            checkpoint.state = :step_3
          when :step_3
            load_friends_of_followers
            $stderr.puts("=== find_shared_followers(3), state = #{checkpoint.state}")
            checkpoint.state = :step_4
          else
            puts("arrived with checkpoint state = #{checkpoint.state}")
            # don't delete checkpoint yet -- we might add more steps
            # checkpoint.delete
          end
        end
      rescue => e
        log_error(e)
        raise
      end
    end

    def log_error(e)
      File.open("log/twitter_errors.log", "a") {|f| f.puts("#{e.class}: #{e}")}
    end

    def convert_target_name_to_id(name)
      user = rate_limiter.with_enhanced_calm do
        twitter_client.user(name)
      end
      @target_id = user.attrs["id"]
      @target_id_symbol = Fact::Symbol.intern(@target_id.to_s)
    end

    def load_followers_of_target
      with_checkpoint("TwitterLoader##{__method__}(#{target_id})", -1) do |checkpoint|
        while (checkpoint.state != 0) do
          response = rate_limiter.with_enhanced_calm do
            twitter_client.follower_ids(self.target_id, :cursor => checkpoint.state)
          end
          ActiveRecord::Base.silence do
            response.ids.each do |follower_id|
              Fact::Statement.create_tuple(target_id_symbol, IS_FOLLOWED_BY_SYMBOL, follower_id, SORTIE_SYMBOL)
            end
          end
          checkpoint.state = response.next_cursor
        end
        checkpoint.delete
      end
    end

    def load_friends_of_followers
      followers = Fact::Statement.where(:subject_id => self.target_id_symbol.id, :predicate_id => IS_FOLLOWED_BY_SYMBOL.id)
      row_count = followers.count
      with_checkpoint("TwitterLoader##{__method__}(#{target_id})", 0) do |checkpoint|
        while (checkpoint.state < row_count) do
          row = followers.offset(checkpoint.state).limit(1).first
          $stderr.puts("=== load friends of follower #{row.target.name} (#{checkpoint.state}/#{row_count})")
          load_friends_of_follower(row.target.name)
          checkpoint.state += 1
        end
        checkpoint.delete
      end
    end

    def load_friends_of_follower(follower_id)
      follower_id_symbol = Fact::Symbol.intern(follower_id)
      with_checkpoint("TwitterLoader##{__method__}(#{target_id})", -1) do |checkpoint|
        while (checkpoint.state != 0) do
          response = rate_limiter.with_enhanced_calm do
            with_retries(:retry => [Twitter::Error::ServiceUnavailable, 
                                    Faraday::Error::ConnectionFailed,
                                    Errno::EADDRNOTAVAIL], 
                         :ignore => Twitter::Error::Unauthorized,
                         :verbose => true) {
              twitter_client.friend_ids(follower_id.to_i, :cursor => checkpoint.state)
            }
          end
          if response
            $stderr.puts("=== loading #{response.ids.count} friends of #{follower_id}")
            ActiveRecord::Base.silence do
              response.ids.each do |friend_id|
                Fact::Statement.create_tuple(friend_id, IS_FOLLOWED_BY_SYMBOL, follower_id_symbol, SORTIE_SYMBOL)
              end
            end
            checkpoint.state = response.next_cursor
          else
            checkpoint.state = 0 # force break
          end
        end
        checkpoint.delete
      end
    end

    # ================================================================
    # Twitter requires rate limiting for certain operations.  RateLimiter
    # assumes we want to query as often as possible, so it waits a "fair"
    # amount of time in the relax() method.

    class RateLimiter

      attr_reader :client, :updated_at, :will_reset_at, :hit_limit
      attr_accessor :hit_count

      def initialize(client)
        $stderr.puts("=== initialize")
        @client = client
        reset
      end

      def reset
        $stderr.puts("=== reset")
        @updated_at = @will_reset_at = @hit_limit = nil
        @hit_count = 0
      end

      def with_enhanced_calm
        self.hit_count += 1
        sleep_if_needed
        # If the yield raises an error, hit_count may be prematurely incremented,
        # but that's probably okay.
        yield
      end

      def sleep_if_needed
        update_rate_limit_status_if_needed
        hits_remaining = hit_limit - hit_count
        $stderr.puts("=== hit_count = #{hit_count}, hits_remaining = #{hits_remaining}, seconds_remaining = #{will_reset_at - Time.zone.now}")
        if (hits_remaining <= 0)
          # no more hits: wait until reset time
          sleep_until(will_reset_at)
        else
          # ideal_time ramps from updated_at to will_reset_at as
          # hit_count goes from 0 to hit_limit.
          ideal_time = linear_intepolate(hit_count, 0, hit_limit, updated_at, will_reset_at)
          sleep_until(ideal_time)
        end
      end

      def linear_intepolate(x, x0, x1, y0, y1)
        return y0 + (x - x0) * (y1 - y0) / (x1 - x0)
      end

      # Return immediately if t is in the past, otherwise sleep
      def sleep_until(t)
        dt = (t - Time.zone.now).ceil        
        $stderr.puts("=== sleep_until(#{t})(#{dt} seconds)")
        sleep(dt) if dt > 0.0
      end

      def update_rate_limit_status_if_needed
        now = Time.zone.now
        if (will_reset_at.nil? || will_reset_at <= now)
          status = client.rate_limit_status
          $stderr.puts("=== new rate_limit_status = #{status.inspect}")
          @updated_at = now
          @will_reset_at = status.reset_time
          @hit_limit = status.remaining_hits
          @hit_count = 0
        end
      end

    end

  end

end
