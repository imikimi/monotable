# encoding: BINARY

# PathStore manages all the storage in one specified path

module MonoTable
  class PathStore
    attr_accessor :path

    # TODO: switch tousing the JournalManager
    attr_accessor :journal_manager
    attr_accessor :chunks # hash keyed by filename
    attr_accessor :local_store

    def initialize(path,options={})

      @local_store=options[:local_store] || LocalStore.new(:store_paths=>[path])
      @path = File.expand_path(path)
      @journal_manager = JournalManager.new(path,self)
      validate_store
      @next_chunk_number=0
      init_chunks
    end

    def max_chunk_size; @local_store.max_chunk_size; end
    def max_index_block_size; @local_store.max_index_block_size; end

    def validate_store
      journal_manager.compact
    end

    def journal
      journal_manager
    end

    # Takes a MemoryChunk object, assigns it a filename, saves it to disk,
    # creates a DiskChunk object, adds that to this pathstore, and returns the DiskChunk
    def add(chunk)
      case chunk
      when MemoryChunk then
        filename=generate_filename
        chunk.save(filename)
        chunks[filename]=DiskChunk.new(:filename=>filename,:journal=>journal_manager,:path_store=>self)
      when DiskChunk then
        raise "DiskChunk attached to some other PathStore" unless !chunk.path_store || chunk.path_store==self
        chunk.path_store=self
        chunks[chunk.filename]=chunk
        local_store.add(chunk)
      else raise "unknown type #{chunk.class}"
      end
    end

    #**************************************
    # internal API
    #**************************************
    def init_chunks
      @chunks={}
      Dir.glob(File.join(path,"*#{CHUNK_EXT}")) do |f|
        f[/\/([0-9]+)\#{CHUNK_EXT}$/]
        chunk_number=$1.to_i
        @next_chunk_number = chunk_number+1 if chunk_number >= @next_chunk_number

        chunks[f]=DiskChunk.new(:filename=>f,:journal=>journal_manager,:path_store=>self)
      end
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
