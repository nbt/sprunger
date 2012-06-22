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
  # tl = ETL::TwitterLoader.new("SwatchUS")
  # tl.find_shared_followers

  require 'twitter'
  require 'with_checkpoint'
  require 'with_retries'

  class TwitterLoader < ETL::Base
    include WithCheckpoint
    include WithRetries

    attr_reader :twitter_client, :topic_name, :rate_limiter

    IS_FOLLOWED_BY_SYMBOL = Fact::Symbol.intern("is followed by")
    SORTIE_SYMBOL = Fact::Symbol.intern("Friends of Celebrity sortie 1.0")

    DEFAULT_AUTHORIZATION = {
      :oauth_token => "7477802-zGgEK0FLcHrXNZYPfvOQmha31k8cUzpP6jh1Zpst74",
      :oauth_token_secret => "JMncmgj6gHnllGfEcKsS7oIlikiq8dElEcCxIHWnmA",
      :consumer_key => "xwiCZr2DoCkbC1RFBqpdA",
      :consumer_secret => "evVgk6XTn6XnUl3dHyi3yvYKnLTyksOfggCokmLU8fM"
    }
    
    # Authorization is an options hash passed to Twitter::Client.new
    def initialize(topic_name, authorization = DEFAULT_AUTHORIZATION)
      @topic_name = topic_name
      @twitter_client = ::Twitter::Client.new(authorization)
      @rate_limiter = RateLimiter.new(@twitter_client, :verbose => true)
    end

    def find_shared_followers
      begin
        rate_limiter.reset
        with_checkpoint("TwitterLoader##{__method__}(#{self.topic_name})", :step_2) do |checkpoint|
          $stderr.puts("=== find_shared_followers(0), state = #{checkpoint.state}")
          case checkpoint.state
          when :step_2
            load_followers_of_topic
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

    def topic_id
      @topic_id ||= convert_topic_name_to_id
    end

    def convert_topic_name_to_id
      user = rate_limiter.with_enhanced_calm { twitter_client.user(self.topic_name) }
      user.attrs["id"]
    end

    def topic_id_symbol
      @topic_id_symbol ||= Fact::Symbol.intern(self.topic_id.to_s)
    end

    def log_error(e)
      File.open("log/twitter_errors.log", "a") {|f| f.puts("#{e.class}: #{e}")}
    end

    def load_followers_of_topic
      with_checkpoint("TwitterLoader##{__method__}(#{topic_id})", -1) do |checkpoint|
        while (checkpoint.state != 0) do
          response = rate_limiter.with_enhanced_calm do
            twitter_client.follower_ids(self.topic_id, :cursor => checkpoint.state)
          end
          if response
            follower_ids = response.ids.map(&:to_s)
            ActiveRecord::Base.silence do
              Fact::Statement.create_tuples(topic_id_symbol, IS_FOLLOWED_BY_SYMBOL, follower_ids, SORTIE_SYMBOL)
            end
          end
          checkpoint.state = response.next_cursor
        end
        checkpoint.delete
      end
    end

    def load_friends_of_followers
      followers = Fact::Statement.where(:subject_id => self.topic_id_symbol.id, :predicate_id => IS_FOLLOWED_BY_SYMBOL.id)
      row_count = followers.count
      with_checkpoint("TwitterLoader##{__method__}(#{topic_id})", 0) do |checkpoint|
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
      with_checkpoint("TwitterLoader##{__method__}(#{topic_id},#{follower_id})", -1) do |checkpoint|
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
            # stringify friend_ids (else create_tuples will interpret them as symbol ids)
            friend_ids = response.ids.map(&:to_s)
            ActiveRecord::Base.silence do
              before_count = Fact::Statement.count
              Fact::Statement.create_tuples(friend_ids, IS_FOLLOWED_BY_SYMBOL, follower_id_symbol, SORTIE_SYMBOL)
              after_count = Fact::Statement.count
              $stderr.puts("=== found #{friend_ids.count} friends of #{follower_id}, (#{after_count - before_count} new).")
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
    # assumes we want to query as often as possible, so with_enhanced_calm
    # waits for the "fair" amount of time before processing the body.

    class RateLimiter

      attr_reader :client, :updated_at, :will_reset_at, :hit_limit
      attr_accessor :hit_count

      def initialize(client, options = {})
        $stderr.puts("=== initialize")
        @client = client
        @verbose = options[:verbose]
        reset
      end

      def blather(msg)
        $stderr.puts("=== " + msg) if @verbose
      end

      def reset
        blather("reset")
        @updated_at = @will_reset_at = @hit_limit = nil
        @hit_count = 0
      end

      def with_enhanced_calm
        sleep_until(ideal_hit_time)
        # If the yield raises an error, hit_count may increment even
        # if twitter doesn't register it, but that's appropriately
        # conservative.
        self.hit_count += 1
        yield
      end

      # Return the time at which we should hit the twitter server with
      # a query, following a linear ramp from the time we update the
      # limit status to twitter's reset time.
      def ideal_hit_time
        while (hit_limit.nil? || (hit_limit - hit_count) <= 0) do
          update_rate_limit_status
          # + 1.0 to account for clock skew between computers
          sleep_until(@will_reset_at + 1.0) if (hit_limit == 0)
        end
        blather("hits_remaining = #{hit_limit - hit_count}, seconds_remaining = #{will_reset_at - Time.zone.now}")
        # ideal_time ramps from updated_at to will_reset_at as
        # hit_count goes from 0 to hit_limit.
        linear_intepolate(hit_count, 0, hit_limit, updated_at, will_reset_at)
      end

      def linear_intepolate(x, x0, x1, y0, y1)
        return y0 + (x - x0) * (y1 - y0) / (x1 - x0)
      end

      # Return immediately if t is in the past, otherwise sleep
      def sleep_until(t)
        dt = (t - Time.zone.now).ceil        
        blather("sleep_until #{t} (#{dt} seconds)")
        sleep(dt) if dt > 0.0
      end

      def update_rate_limit_status
        now = Time.zone.now
        status = client.rate_limit_status
        blather("rate_limit_status = #{status.inspect}")
        @updated_at = now
        @will_reset_at = status.reset_time
        @hit_limit = status.remaining_hits
        @hit_count = 0
      end

    end

  end

end
