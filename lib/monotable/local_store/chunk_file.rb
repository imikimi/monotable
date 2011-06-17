# encoding: BINARY
require "fileutils"
require 'digest/md5'

module MonoTable

  class ChunkFile < Entry
    attr_accessor :file_handle
    attr_accessor :path_store
    attr_accessor :journal
    attr_accessor :max_chunk_size

    def initialize(fn,options={})
      init_entry
      self.file_handle=FileHandle.new(fn)
      @max_chunk_size = options[:max_chunk_size] || DEFAULT_MAX_CHUNK_SIZE
      @path_store=options[:path_store]
      @journal=options[:journal] || (path_store && path_store.journal) || Journal.new(fn+".testing_journal")
      load_index_block

    end

    # load the value from file
    def [](key)
      get(key)
    end

    def filename
      @file_handle.filename
    end

    def load_index_block
      parse_all_entries(file_handle)
      # SBD 2011-05-04 - I think ChunkFile should never think about multiple entries. It should just expect and load one entry in a chunk-file. Only journals have multiple entries, and the journal compaction code should handle that.
      #parse_all_entries(journal.journal_file)
    end

    def parse_all_entries(file_handle)
      return unless file_handle.exists?
      file_handle.read do |f|
        # read the first chunk (faster to do it this way)
        while !f.eof?
          entry = Entry.new(f,file_handle)
          apply_entry(entry)
        end
      end
    end

    # "reload" - re-read/reset all member-variables from the chunk-file on disk
    def reset
      #TODO
    end

    #*************************************************************
    # Read API
    #*************************************************************
    def get(key,columns=nil)
      (h=@records[key]) && h.fields(columns)
    end

    #*************************************************************
    # Write API
    #*************************************************************
    # NOTE: The "update" method inherited from Entry works. No need to re-implement.
    def set(key,columns)
      ret=
        set_internal(key,
        journal.set(file_handle,key,columns)
        )
      EventQueue<<ChunkFullEvent.new(self) if size > max_chunk_size
      ret
    end

    def delete(key)
      journal.delete(file_handle,key)
      delete_internal(key)
    end

    #*************************************************************
    # Internal API
    #*************************************************************
    # all keys >= on_key are put into a new chunk
    def split(on_key=nil,to_filename=nil)
      on_key,size1,size2=middle_key_and_sizes(on_key)
      to_filename||=path_store.generate_filename

      # create new chunk
      second_chunk_file=ChunkFile.new(to_filename,:journal=>journal,:max_chunk_size=>max_chunk_size)

      # do the actual split
      # NOTE: this just splits the in-memory Records. If they are DiskRecords, they will still point to the same file, which is correct for reading.
      self.split_into(on_key,second_chunk_file)

      # update the path_store (which will also update the local_store
      path_store.add(second_chunk_file) if path_store

      # set entry
      journal.split(file_handle,on_key,to_filename)

      # update sizes
      self.size=size1
      second_chunk_file.size=size2

      # return the new ChunkFile object
      second_chunk_file
    end

    def middle_key_and_sizes(mkey)
      half_size=size/2
      size1=0
      size2=0

      if mkey
        # already have a key
        records.each do |k,v|
          vsize=v.size
          if k < mkey
            size1+=vsize
          else
            size2+=vsize
          end
        end
      else
        # determine the middle-most key
        records.each do |k,v|
          vsize=v.size
          if !mkey && size1+vsize>half_size
            mkey=k
          elsif size1 < half_size
            size1+=vsize
          else
            size2+=vsize
          end
        end
      end
      [mkey,size1,size2]
    end
  end
end
