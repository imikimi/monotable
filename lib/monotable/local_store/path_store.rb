# encoding: BINARY

# PathStore manages all the storage in one specified path

module Monotable
  class PathStore
    attr_accessor :path

    # TODO: switch tousing the JournalManager
    attr_accessor :journal_manager
    attr_accessor :chunks # hash keyed by filename
    attr_accessor :local_store

    def initialize(path,options={})

      @local_store=options[:local_store] || LocalStore.new(:store_paths=>[path])
      @path = File.expand_path(path)
      @journal_manager = JournalManager.new(path,options.merge(:path_store=>self))
      validate_store
      @next_chunk_number=0

      init_chunks(options)

      load_config || create_config
    end

    # the config file and path for this path_store instance
    def config_filename
      File.join path,LOCAL_STORE_CONFIG_FILE
    end

    def accounting_size
      @chunks.inject(0) {|total,keychunk| keychunk[1].accounting_size + total}
    end

    def record_count
      @chunks.inject(0) {|total,keychunk| keychunk[1].length + total}
    end

    # load the config file on disk if it exists
    # returns true if the file was loaded, false if it didn't exist
    def load_config
      return unless File.exists?(config_filename)
      @config_file = YAML.load_file(config_filename)
    end

    # create and save the default config file
    def create_config
      raise InternalError.new("config already exists") if File.exists?(config_filename)
      File.open(config_filename,"w") {|f| f.write default_config_file.to_yaml}
    end

    # create the default config file
    def default_config_file
      {
      :config_for => "monotable_store",
      :created => {
        :daemon_type => "monotable",
        :daemon_version => Monotable::VERSION,
        :created_at => Time.now.to_s,
        :runtime_environment => {
          :ruby_version => RUBY_VERSION,
          :ruby_platform => RUBY_PLATFORM,
          :ruby_release_date => RUBY_RELEASE_DATE
          }
        }
      }
    end

    def store_initialized?
      puts "testing path_store #{path}"
      Dir.glob(File.join(path,"*")).each do |file|
        puts "   file: #{file}"
      end
    end

    def max_chunk_size; @local_store.max_chunk_size; end
    def max_index_block_size; @local_store.max_index_block_size; end

    def validate_store
      journal_manager.compact
    end

    # options: see Journal#compact for more info
    def compact(options={})
      journal_manager.compact(options)
    end

    def journal
      journal_manager
    end

    # Takes a MemoryChunk object, assigns it a filename, saves it to disk,
    # creates a DiskChunk object, adds that to this pathstore, and
    # returns the DiskChunk
    def add_chunk(chunk)
      case chunk
      when MemoryChunk then
        filename = generate_filename
        chunk.save(filename)
        chunks[filename] = DiskChunk.init(:filename=>filename,:journal=>journal_manager,:path_store=>self)
      when String then
        filename = generate_filename
        File.open(filename,"wb") {|f| f.write(chunk)}
        chunks[filename] = DiskChunk.init(:filename=>filename,:journal=>journal_manager,:path_store=>self)
      when DiskChunk then
        raise "DiskChunk attached to some other PathStore" unless chunk.path_store==self
        chunks[chunk.filename] = chunk
      else raise "unknown type #{chunk.class}"
      end
    end

    #**************************************
    # internal API
    #**************************************
    def init_chunks(options)
      $stdout.write "  PathStore(#{path}) initializing chunks: " if options[:verbose]
      @chunks={}
      Dir.glob(File.join(path,"*#{CHUNK_EXT}")) do |f|
        f[/\/([0-9]+)\.#{CHUNK_EXT}$/]
        chunk_number=$1.to_i
        @next_chunk_number = chunk_number+1 if chunk_number >= @next_chunk_number

        chunks[f]=DiskChunk.init(:filename=>f,:journal=>journal_manager,:path_store=>self)
        $stdout.write "." if options[:verbose]
      end
      $stdout.puts "" if options[:verbose]
    end

    # select a numbered filename between 1 and 100 million that is unique on this path
    def generate_filename
      while true
        filename=File.join(path,"%08d#{CHUNK_EXT}" % (@next_chunk_number % 100_000_000))
        return filename unless chunks[filename] # if unique, return
        @next_chunk_number+=1
      end
    end
  end
end
