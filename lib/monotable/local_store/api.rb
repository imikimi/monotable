module Monotable
  module ReadAPI

    # returns:
    #   exists: {hash-of-fields}
    #   !exists: nil
    def [](key) (record=get_record(key)) && record.fields; end

    # returns:
    #   exists: {:record=>{hash-of-fields}, ...}
    #   !exist: {:record=>nil}
    def get(key,columns=nil)
      record=get_record(key)
      if record
        {:record=>record.fields(columns), :size=>record.accounting_size, :num_fields=>record.fields.length}
      else
        {:record=>nil}
      end
    end
  end

  module WriteAPI
    def []=(key,fields) set(key,fields) end
  end
end
