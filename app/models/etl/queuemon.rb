class QueueMon

  def self.mon
    while true do
      display_queue
      sleep(3)
    end
  end

  def self.display_queue
    clear_screen
    Delayed::Job.all.sort {|a, b| a.id <=> b.id}.each {|j| display_job(j) }
  end
  
  def self.clear_screen
    print("\033[H\033[2J")
  end

  def self.display_job(j)
    printf("id:%04d attempts:%d handler:%s locked_by:%s\n", j.id, j.attempts, j.handler.gsub("\n","\\n"), j.locked_by)
  end

end
