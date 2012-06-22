module WithRetries
  extend self
  
  WITH_RETRIES_DEFAULT_OPTIONS = {
    :ignore => [],
    :retry => [],
    :max_retries => 3,
    :delay_exponent => 3.0,
    :verbose => true
  }

  def with_retries(options = {}, &block)
    options = WITH_RETRIES_DEFAULT_OPTIONS.merge(options)
    retries = 0
    while true do
      begin
         return yield
      rescue *options[:ignore] => e
        $stderr.puts("=== ignoring #{e.class}: #{e.message}") if options[:verbose]
        return
      rescue *options[:retry] => e
        $stderr.puts("=== rescuing #{e.class}: #{e.message} (retry = #{retries}/#{options[:max_retries]})") if options[:verbose]
        raise if (retries >= options[:max_retries])
        if (options[:delay_exponent] > 0.0) 
          delay_time = options[:delay_exponent] ** retries
          sleep delay_time
        end
        retries += 1
      end
    end
  end

end

