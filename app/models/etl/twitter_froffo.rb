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

      ActiveRecord::Base.silence do
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
    end

    def self.analyze_friends_of_followers_of(a_twitter_name, top_n)
      ignored, a_twitter_id, a_follower_count = get_twitter_user_info(a_twitter_name)

      # NOTE: this is indescriminite and assumes the DB only contains
      # entries for followers of twitter_name.  Needs to do a join.
      relation = Fact::Statement.select("count(*) AS count_all, subject_id AS subject_id").
        group(:subject_id).
        order("count_all desc").
        limit(top_n)
      relation.each do |r| 
        b_twitter_id = Fact::Symbol.find(r.subject_id).name.to_i
        b_twitter_name, ignored, b_follower_count = get_twitter_user_info(b_twitter_id)
        fof_count = r.count_all.to_i

        print_report(a_twitter_name, a_twitter_id, a_follower_count, b_twitter_name, b_twitter_id, b_follower_count, fof_count)
      end
    end

    def self.print_report(a_twitter_name, a_twitter_id, a_follower_count, b_twitter_name, b_twitter_id, b_follower_count, fof_count)
      printf("%10s, %10s, %7d, %10s, %10s, %7d, %7d\n", 
             a_twitter_name, a_twitter_id, a_follower_count, b_twitter_name, b_twitter_id, b_follower_count, fof_count)
    end

    # return [name, id, followers_count, friends_count]
    def self.get_twitter_user_info(id_or_name)
      sleep(4.0)                # awful hack: Twitter.user() is rate limited.
      resp = Twitter.user(id_or_name)
      [resp["name"], resp["id"], resp["followers_count"], resp["friends_count"]]
    end

    def self.twitter_id_of(twitter_name)
      Twitter.user(twitter_name)["id"]
    end

    def self.load_followers_of(twitter_id)
      Delayed::Job.enqueue TwitterJob.new(:load_followers_of, twitter_id)
      wait_for_delayed_job(0, "load_followers_of")
    end

    def self.load_friends_of(twitter_id)
      wait_for_delayed_job(15, "load_friends_of")
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
