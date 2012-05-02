# TODO:
# * object_id => target_id
# cached symbols?
# store IDs, not screen names
# * design & implement with_checkpoint
# recover & retry twitter_client errors
# spawn per-client / account process

module ETL

  # Load things into Fact table via Twitter API.
  # see: 
  # https://dev.twitter.com/docs
  # https://dev.twitter.com/docs/using-search
  
  # Goal: 
  # Phase 1: find all the Twitter followers for a given celebrity.
  # Phase 2: find all the tweets from those followers
  # Phase 3: do word analysis on those tweets to look for common interests (excluding the celebrity)
  #
  # For phase 1:
  # Find all twitter screen_names whose stated name matches celebrity name
  # For each twitter screen_name, create fact:
  #    [celebrity_name]["has twitter screen_name"][twitter_screen_name]["f.o.c. sortie 1.0"]
  # For each follower of each twitter screen_name, create fact:
  #    [twitter_screen_name]["is followed by twitter screen_name"][twitter_screen_name]["f.o.c. sortie 1.0"]

  # SYNOPSIS:
  # oauth_token = "7477802-zGgEK0FLcHrXNZYPfvOQmha31k8cUzpP6jh1Zpst74"
  # oauth_token_secret = "JMncmgj6gHnllGfEcKsS7oIlikiq8dElEcCxIHWnmA"
  # consumer_key = "xwiCZr2DoCkbC1RFBqpdA"
  # consumer_secret = "evVgk6XTn6XnUl3dHyi3yvYKnLTyksOfggCokmLU8fM"
  # tl = ETL::TwitterLoader.new(:oauth_token => oauth_token, :oauth_token_secret => oauth_token_secret, :consumer_key => consumer_key, :consumer_secret => consumer_secret)
  # name = "Charles Bukowski"
  # tl.load_all_ids_referring_to(name)
  # tl.load_followers_of(name)

  class TwitterClient
    attr_accessor :username, :consumer_key, :consumer_secret, :oauth_token, :oauth_token_secret
  end

  class TwitterLoader < ETL::Base
    require 'twitter'

    attr_reader :twitter_client

    MAX_USERS_PER_SEARCH = 20
    HAS_TWITTER_ACCOUNT_ID = "has twitter account id"
    SORTIE_NAME = "F.O.C. sortie 1.0"  

    # Authorization is an optons hash passed to Twitter::Client.new
    def initialize(authorization = {})
      @twitter_client = ::Twitter::Client.new(authorization)
    end

    def load_all_ids_referring_to(name)
      name_symbol = Fact::Symbol.intern(name)
      predicate_symbol = Fact::Symbol.intern(HAS_TWITTER_ACCOUNT_ID)
      context_symbol = Fact::Symbol.intern(SORTIE_NAME)

      page = 0
      has_more = true
      while (has_more) do
        with_rate_limiting do 
          # GET users/search
          twitter_users = with_network_retries() {
            twitter_client.user_search(name, :page => page, :per_page => MAX_USERS_PER_SEARCH)
          }
          twitter_users.each do |twitter_user| 
            if (twitter_user.name.downcase == name.downcase)
              Fact::Statement.create_tuple(name_symbol, predicate_symbol, twitter_user.id, context_symbol)
            end
          end
          has_more = twitter_users.size == MAX_USERS_PER_SEARCH
          page += 1
        end
      end
    end

    def load_followers_of(name)
      name_symbol = Fact::Symbol.intern(name)
      predicate_symbol = Fact::Symbol.intern(HAS_TWITTER_ACCOUNT_ID)
      r = Fact::Statement.where(:subject_id => name_symbol.id, :predicate_id => predicate_symbol.id)
      $stderr.puts("=== found #{r.count} accounts referring to #{name}")
      r.each do |fact|
        # fact has {:subject => 'real name', :target => 'screen_name'}
        load_followers_of_id(fact.target.name)
      end
    end

    def load_followers_of_id(user_id)
      checkpoint = Checkpoint.new("twitter_cursor_" + user_id.to_s, -1)
      $stderr.puts("=== load_followers_of user_id #{user_id}")
      while (checkpoint.value != 0) do
        $stderr.puts("=== load_followers_of user_id #{user_id}, checkpoint = #{checkpoint.value}")
        with_rate_limiting do
          resp = with_network_retries(:ignored => Twitter::Error::Unauthorized) {
            # GET followers/ids
            twitter_client.follower_ids(:user_id => user_id, :cursor => checkpoint.value)
          }
          if (resp)
            $stderr.puts("=== user_id #{user_id} has #{resp.ids.count} followers")
            load_followers_from_ids(user_id, resp.ids)
            checkpoint.value = resp.next_cursor
          else
            checkpoint.value = 0
          end
        end
      end
      checkpoint.clear
    end

    MAX_USERS_PER_LOOKUP = 100

    # ids is an array of user IDs (returned by GET followers/ids)
    def load_followers_from_ids(user_id, follower_ids)
      subject = Fact::Symbol.intern(user_id)
      predicate = Fact::Symbol.intern("is followed by twitter id")
      follower_ids.each do |follower_id|
        Fact::Statement.create_tuple(subject, predicate, follower_id, SORTIE_NAME)
      end
    end

    # ================================================================
    # refactor

    # :ignored (rescued and ignored)
    # :retried (rescued and retried)
    def with_network_retries(options = {}, &block)
      access_network(block, retries = 0, options)
    end

    def access_network(proc, retries = 0, options = {})
      begin
        proc.call
      rescue options[:ignored] => e
        $stderr.puts("=== ignoring #{e.class}: #{e.message}")
        return nil
      rescue options[:retried] => e
        raise if (retry_count >= MAX_RETRIES)
        $stderr.puts("=== retrying #{e.message}")
        sleep 3**retry_count
        access_network(proc, retries + 1, options)
      end
    end

    def with_rate_limiting(&block)
      rate_limiter.relax
      ActiveRecord::Base.silence do 
        yield
      end
    end

    def rate_limiter
      @rate_limiter ||= RateLimiter.new(twitter_client)
    end

    # ================================================================
    # Twitter requires rate limiting for certain operations.  RateLimiter
    # assumes we want to query as often as possible, so it waits a "fair"
    # amount of time in the relax() method.

    class RateLimiter

      attr_reader :client, :updated_at, :will_reset_at, :remaining_hits

      def initialize(client)
        @client = client
      end

      def needs_status_update?
        will_reset_at.nil? || remaining_hits.nil? || (will_reset_at <= Time.zone.now) || (remaining_hits <= 0)
      end

      def update_status
        status = client.rate_limit_status
        $stderr.puts("=== update_rate_limit_status = #{status.inspect}")
        @updated_at = Time.zone.now
        @will_reset_at = status.reset_time
        @remaining_hits = status.remaining_hits
      end

      def sleep_time
        update_status if needs_status_update?
        if (remaining_hits == 0)
          will_reset_at - Time.zone.now
        else
          (will_reset_at - updated_at) / remaining_hits
        end
      end

      def relax      
        st = sleep_time
        $stderr.puts("=== relaxing for #{st}")
        sleep(st)
      end

    end

  end

end
