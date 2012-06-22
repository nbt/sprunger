module ETL

  # Load "all friends of followers of x":
  #   load all followers of x
  #   loop for i in followers:
  #     load friends of follower[i]
  #   end

  class TwitterFrOfFo

    IS_FOLLOWED_BY_SYMBOL = Fact::Symbol.intern("is followed by")
    SORTIE_SYMBOL = Fact::Symbol.intern("Friends of Celebrity sortie 1.1")

    def self.load_friends_of_followers_of(twitter_name)
      ETL::TwitterProcessor.reset_all

      twitter_id = twitter_id_of(twitter_name)
      load_followers_of(twitter_id)
      followers = Fact::Statement.where(:subject_id => Fact::Symbol.to_id(twitter_id.to_s),
                                        :predicate_id => Fact::Symbol.to_id(IS_FOLLOWED_BY_SYMBOL),
                                        :context_id => Fact::Symbol.to_id(SORTIE_SYMBOL))
      followers.count.times do |i|
        target = followers.offset(i).limit(1).first.target
        load_friends_of(target.name)
      end
      wait_for_delayed_job(0, :load_friends_of_followers)
    end

    def self.twitter_id_of(twitter_name)
      Twitter.user(twitter_name)["id"]
    end

    def self.load_followers_of(twitter_id)
      Delayed::Job.enqueue TwitterJob.new(:load_followers_of, twitter_id)
      wait_for_delayed_job(0, "load_followers_of")
    end

    def self.load_friends_of(twitter_id)
      wait_for_delayed_job(10, "load_friends_of")
      Delayed::Job.enqueue TwitterJob.new(:load_friends_of, twitter_id)
    end

    def self.wait_for_delayed_job(queue_size, message)
      while (Delayed::Job.count > queue_size) do
        print("\nwaiting for #{message} to complete...")
        sleep(5.0)
      end
      puts("done!")
    end

  end

end
