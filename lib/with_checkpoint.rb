# A simple checkpointing mechanism.  Upon entry, with_checkpoint looks
# for a checkpoint with the given name and restores state from it if
# found, creating it otherwise.  Upon exit, it saves the checkpoint
# unless the checkpoint has been deleted.

# Sample usage:
#
# class TestCheck
#   extend WithCheckpoint
#
#   def self.do_it
#     with_checkpoint(:fred, [0]) {|checkpoint|
#       puts("intial state = #{checkpoint.state}")
#       while (checkpoint.state[0] < 200) do
#         raise RuntimeError if rand > 0.99
#         checkpoint.state = [checkpoint.state[0]+1]
#       end
#       puts("completed normally, deleting checkpoint")
#       checkpoint.delete
#     }
#   end
#
# end


module WithCheckpoint

  def with_checkpoint(name, initial_state, &body)
    r = Checkpoint.where(:name => name)
    # fetch existing or create fresh checkpoint
    checkpoint = r.exists? ? r.first : r.new(:state => initial_state)
    begin
      yield(checkpoint)
    ensure
      # upon leaving the body, save the checkpoint iff needed
      checkpoint.save unless checkpoint.destroyed?
    end
  end
end
