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

      @file_system = options[:file_system] || Tools::FileSystem.new
      @local_store = options[:local_store] || LocalStore.new(:store_paths=>[path])
      @path = File.expand_path(path)
      @journal_manager = JournalManager.new(path,options.merge(:path_store=>self))
      @next_chunk_number=0

      init_chunks(options)

      load_config || create_config
    end

    # the config file and path for this path_store instance
    def config_filename
      File.join path,LOCAL_STORE_CONFIG_FILE
    end


    # in bytes
    def free_space
      @file_system.free_space(path)
    end

    def contains_chunk?(chunk)
      contains_file? chunk.filename
    end

    def contains_file?(full_file_path)
      full_file_path[0..(path.length-1)] == path
    end

    def accounting_size
      @chunks.inject(0) {|total,keychunk| keychunk[1].accounting_size + total}
    end

    def record_count
      @chunks.inject(0) {|total,keychunk| keychunk[1].length + total}
    end

    def status
      {
      :chunk_count => @chunks.length,
      :accounting_size => accounting_size,
      :record_count => record_count,
      :path => path,
      }
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

    def compact_existing_journals
      journal_manager.compact_existing_journals
    end

    # options: see Journal#compact for more info
    def compact(options={},&block)
      journal_manager.compact(options,&block)
    end

    def journal
      journal_manager
    end

    # Takes a MemoryChunk object, assigns it a filename, saves it to disk,
    # creates a DiskChunk object, adds that to this pathstore, and
    # returns the DiskChunk
    def add_chunk(chunk)
      raise InternalError.new("chunk must have its basename set") unless chunk.basename && chunk.basename.length>0
      filename = full_chunk_path(chunk.basename)
      chunks[chunk.basename] = case chunk
        when MemoryChunk then
          #puts "PathStore#add_chunk chunk.range=#{chunk.range.inspect}, filename=#{filename.inspect} chunk.class=#{chunk.class}"
          chunk.save(filename)
          DiskChunk.init(:filename => filename, :journal => journal_manager, :path_store => self)#.tap do |dc|
#            puts "DiskChunk.keys = #{dc.keys.inspect}"
#          end
        when String then
          File.open(filename,"wb") {|f| f.write(chunk)}
          DiskChunk.init(:filename => filename, :journal => journal_manager, :path_store => self)
        when DiskChunk then
          chunk.path_store||=self
          raise "DiskChunk attached to some other PathStore" unless chunk.path_store==self
          chunk
        else raise "unknown type #{chunk.class}"
      end
    end

    # just remove the chunk from the path-store's internal tracking
    def remove_chunk(chunk)
      #puts "#{self.class}#remove_chunk(a) #{chunk.filename} chunks.length=#{chunks.length}"
      chunk.path_store = nil
      chunks.delete chunk.basename
      #puts "#{self.class}#remove_chunk(b) #{chunk.filename} chunks.length=#{chunks.length}"
      chunk
    end

    # delete the chunk from disk
    def delete_chunk(chunk)
      chunk.delete_chunk
      remove_chunk(chunk)
    end

    #**************************************
    # internal API
    #**************************************
    def init_chunks(options)
      $stdout.write "  PathStore(#{path}) initializing chunks: " if options[:verbose]
      @chunks={}
      Dir.glob(File.join(path,"*#{CHUNK_EXT}")) do |f|
        f[/\/([a-f0-9]+)\#{CHUNK_EXT}$/] # match hex string followed by chunk extension
        chunk_number=$1.to_i
        @next_chunk_number = chunk_number+1 if chunk_number >= @next_chunk_number

        add_chunk DiskChunk.init(:filename=>f,:journal=>journal_manager,:path_store=>self)
        $stdout.write "." if options[:verbose]
      end
      $stdout.puts "" if options[:verbose]
    end

    # select a numbered filename between 1 and 100 million that is unique on this path
    def full_chunk_path(basename)
      PathStore.full_chunk_path path, basename
    end

    class << self
      # this abstraction will allow us to break chunks into sub-dirs in the future
      # SBD: we'll almost certainly need it as we expect ~ 64,000 chunks on a 2TB disk with a 64-meg chunk-size
      def full_chunk_path(base_path, basename)
        File.join base_path, basename
      end
    end
  end
end
