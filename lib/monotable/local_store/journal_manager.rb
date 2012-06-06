=begin
The JournalManager should create new journals as needed.
It should guarantee the sequentiality. It should never re-use a journal ID. The reason for this is on crash recovery, we have to
know the order the journals should be replayed in.

The journal manager may also need to manage the fact that we may limit the number of chunks per journal. This is important because
chunk compaction needs to load all chunks touched by a journal fully into memory.

If we have multiple active journals, the changes will be comitted in an out-of-order way... I -think- this can be proven to be safe
as long as all writes to one chunk are done in the same journal. I -think- this can also be safe if there chunk splits or merges.

On a split, the origianl chunk is kept, though at half size, and the new chunk is created. As it is new, any writes to it will be assigned
to the newest active journal - and hence is guaranteed to be comitted AFTER the split.

On merges... We may need to be careful. If the second chunk's journal comes after the first chunks, then after the merge, all writes must
be in a journal >= the second chunks. Firther, the merge must be in a journal >= the second chunks.

THREADSAFE (not yet)

  JournalManager should be made to be threadsafe. Journals, however, will not be threadsafe. They will rely on their manager
  to ensure single access.
=end

module Monotable
  class JournalManager
    attr_accessor :current_journal
    attr_accessor :journal_number
    attr_accessor :path
    attr_accessor :path_store
    attr_accessor :frozen_journals
    attr_accessor :max_journal_size

    def initialize(path,options={})
      @max_journal_size=options[:max_journal_size]
      @path_store = options[:path_store] || PathStore.new(path)
      @path=path
      @journal_number=0
      @frozen_journals=[]
      new_journal
    end

    #**********************************************
    #**********************************************
    def local_store
      path_store.local_store
    end

    def freeze_journal(journal)
      if current_journal==journal
        @frozen_journals<<journal
        new_journal
      end
    end

    # options:    see Journal#compact for more information on options
    def compact(options={},&block)
      if @current_journal
        @current_journal.compact(options,&block)
        new_journal
      end

      # reset journal number
      @journal_number=0
    end

    # compact all existing journals on disk
    # options:    see Journal#compact for more information on options
    def compact_existing_journals(options={})
      # make sure we execute the journals in numerical ascending order

      # get all the journal filenames and sort them in ascending numerical order
      journals=Dir[File.join(path,"*#{JOURNAL_EXT}")].collect do |journal_filename|
        journal_filename[/[^0-9]+([0-9]+)[^0-9]#{JOURNAL_EXT}/]
        [$1.to_i,journal_filename]
      end.sort

      # compact the journals in order
      journals.each do |a|
        num,filename=a
        Journal.new(filename,:journal_manager=>self,:max_journal_size=>@max_journal_size).compact(options)
      end
    end

    #**********************************************
    # Journal API
    #**********************************************
    def set(chunk,key,record)
      @current_journal.set(chunk,key,record)
    end

    def delete(chunk,key)
      @current_journal.delete(chunk,key)
    end

    def delete_chunk(chunk)
      @current_journal.delete_chunk(chunk)
    end

    def split(chunk,key,to_filename)
      @current_journal.split(chunk,key,to_filename)
    end

    def move_chunk(chunk,path_store)
      @current_journal.move_chunk(chunk,path_store)
    end

    #**********************************************
    #**********************************************
    private
    def new_journal
      @current_journal=Journal.new(File.join(path,"journal.#{"%08d"%@journal_number+=1}#{JOURNAL_EXT}"), :journal_manager=>self,:max_journal_size=>@max_journal_size)
    end
  end
end
