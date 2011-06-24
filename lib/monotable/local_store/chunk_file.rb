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
      return unless file_handle.exists? # it is legal for the file on disk to not exist - which is equivelent to saying the chunk starts out empty. All writes go to the journal anyway and the file will be created when compaction occures.
      file_handle.read {|f|parse_minimally(f)}
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
      ret=set_internal(key,journal.set(file_handle,key,columns))
      EventQueue<<ChunkFullEvent.new(self) if accounting_size > max_chunk_size
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
      if on_key
        size1,size2=split_on_key_sizes(on_key)
      else
        on_key,size1,size2=middle_key_and_sizes
      end
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
      self.accounting_size=size1 || self.calculate_accounting_size
      second_chunk_file.accounting_size=size2 || second_chunk_file.calculate_accounting_size

      # return the new ChunkFile object
      second_chunk_file
    end

    # returns array: [sizes < on_key, sizes >= on_key]
    def split_on_key_sizes(on_key)
      size1=size2=0
      records.each do |k,v|
        asize=v.accounting_size
        if k < on_key
          size1+=asize
        else
          size2+=asize
        end
      end
      [size1,size2]
    end

    # returns array: [middle_key, sizes < middle_key, sizes >= middle_key]
    def middle_key_and_sizes
      half_size=accounting_size/2
      size1=size2=0
      mkey=nil

      # determine the middle-most key
      records.keys.sort.each do |key|
        v=records[key]
        asize=v.accounting_size
        if size1+(asize/2)>half_size
          mkey=key
          size2+=accounting_size-size1
          break
        end
        size1+=asize
      end
      # Guarantees:
      # if records.length > 0 then size1 is > 0
      # if records.length > 1 then size2 is also > 0
      # size1 + size2 == accounting_size
      [mkey,size1,size2]
    end
  end
end
