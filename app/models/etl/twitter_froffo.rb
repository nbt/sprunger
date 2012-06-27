require 'with_checkpoint'

module ETL

  # Load "all friends of followers of x":
  #   load all followers of x
  #   loop for i in followers:
  #     load friends of follower[i]
  #   end

  class TwitterFrOfFo
    extend WithCheckpoint

    IS_FOLLOWED_BY_SYMBOL = Fact::Symbol.intern("is followed by")
    SORTIE_SYMBOL = Fact::Symbol.intern("Friends of Celebrity sortie 1.1")

    def self.load_friends_of_followers_of(twitter_name)
      ETL::TwitterProcessor.reset_all

      ActiveRecord::Base.silence do
        puts("=== #{__method__}[0]")
        with_checkpoint("#{self.class}.#{__method__}(#{twitter_name}).id", nil) do |id_checkpoint|
          puts("=== #{__method__}[1]")
          if (twitter_id = id_checkpoint.state).nil?
            puts("=== #{__method__}[2]")
            twitter_id = twitter_id_of(twitter_name)
            id_checkpoint.state = twitter_id
          end
          
          puts("=== #{__method__}[3]")
          with_checkpoint("#{self.class}.#{__method__}(#{twitter_name}).step", :step_0) do |step_checkpoint|
            puts("=== #{__method__}[4] (#{step_checkpoint.state})")
            while (step_checkpoint.state != :done)
              puts("=== #{__method__}[5] (#{step_checkpoint.state})")
              case step_checkpoint.state
              when :step_0
                load_followers_of(twitter_id)
                step_checkpoint.state = :step_1
              when :step_1
                load_friends_of_followers_aux(twitter_id)
                step_checkpoint.state = :step_2
              when :step_2
                wait_for_delayed_job(0, "finishing loading friends of followers")
                step_checkpoint.state = :step_3
              when :step_3
                raise ArgumentError.new("need to implement step_3 (checkpoint is still in place).")
              else
                raise ArgumentError.new("unknown checkpoint state = #{step_checkpoint.state}")
              end
            end
          end
          puts("=== #{__method__}[6]")
        end
        puts("=== #{__method__}[7]")
      end
      puts("=== #{__method__}[8]")
    end


    def self.with_logging(msg)
      t0 = Time.now
      $stderr.printf("=== %s...", msg)
      result = yield
      dt = Time.now - t0
      $stderr.printf("[%0.3f s]\n", dt)
      Rails.logger.debug(sprintf("=== %s...[%0.3f s]", msg, dt))
      result
    end

    def self.load_friends_of_followers_aux(twitter_id)
      followers = Fact::Statement.where(:subject_id => Fact::Symbol.to_id(twitter_id.to_s),
                                        :predicate_id => Fact::Symbol.to_id(IS_FOLLOWED_BY_SYMBOL),
                                        :context_id => Fact::Symbol.to_id(SORTIE_SYMBOL))
      self.with_checkpoint("#{self.class}.#{__method__}(#{twitter_id})", 0) do |checkpoint|
        start_time = Time.now
        start_statement_count = Fact::Statement.count
        count = followers.count
        while (checkpoint.state < count)
          sps = sprintf("%0.3f", (Fact::Statement.count - start_statement_count)/(Time.now - start_time))
          with_logging("loading friends_of(#{twitter_id}) (#{checkpoint.state}/#{count}, r/s = #{sps})") {
            target = followers.offset(checkpoint.state).limit(1).first.target
            load_friends_of(target.name)
          }
          checkpoint.state += 1
        end
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
      while (Delayed::Job.count > 15) do
        sleep(5.0)
      end
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
