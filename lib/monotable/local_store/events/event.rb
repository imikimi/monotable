module MonoTable
  class Event
  end

  class ChunkFullEvent < Event
    attr_accessor :chunk
    def initialize(chunk)
      @chunk=chunk
    end
  end

  class JournalFullEvent < Event
    attr_accessor :journal
    def initialize(journal)
      @journal=journal
    end
  end
end
