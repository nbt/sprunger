module ETL

  class Checkpoint

    attr_reader :name, :initial_value

    def initialize(name, initial_value = nil)
      @name = name
      @initial_value = initial_value
    end

    def value
      has_checkpoint? ? get_checkpoint : initial_value
    end

    def value=(value)
      set_checkpoint(value)
    end

    def clear
      clear_checkpoint
    end

    private

    def checkpoint_filename
      "log/#{self.name}.ckp"
    end

    def has_checkpoint?
      File.exists?(checkpoint_filename)
    end

    def set_checkpoint(value)
      File.open(checkpoint_filename, "w") {|f| YAML.dump(value, f)}
    end

    def get_checkpoint
      @value = File.open(checkpoint_filename, "r") {|f| YAML.load(f)}
    end

    def clear_checkpoint
      File.delete(checkpoint_filename) if has_checkpoint?
    end

  end

end

