module Monotable
module ReadAPI

    # returns:
    #   exists: {hash-of-fields}
    #   !exists: nil
    def [](key) get(key)[:record]; end

    # returns:
    #   exists: Record object
    #   !exists: nil
    def get_record(key)
      raise "stub"
    end

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

    # returns array in format: {:records=>[[key,record],...],:next_options}
    # get_first :gte => key
    # options
    #   :limit => # (default = 1)
    #   :gte => key
    #   :gt => key
    #   :lt => key
    #   :lte => key
    #   :with_prefix => key
    #   :columns => nil / {...}
    #
    # Returns
    #   {:records=>[[key,{fields_hash}],[...],...], :next_options=>{...}}
    def get_first(options={})
      raise "stub"
    end

    # returns array in format: [[key,record],...]
    # options
    #   :limit => # (default = 1)
    #   :gte => key
    #   :gt => key
    #   :lt => key
    #   :lte => key
    #   :with_prefix => key
    #   :columns => nil / {...}
    #
    # Returns
    #   {:records=>[[key,{fields_hash}],[...],...], :next_options=>{...}}
    #
    # Examples
    #   store.get_first(:gte=>"key2") => {:records=>[["key2",{"field"=>"value"}]], ...}
    #   store.get_first(:gt=>"key2") => {:records=>[["key3",{"field"=>"value"}]], ...}
    #   store.get_first(:with_prefix=>"key2") => {:records=>[["key3",{"field"=>"value"}]], ...}
    #   store.get_first(:gt=>"key2", :limit=>3) => {:records=>[
    #     ["key3",{"field"=>"value"}],
    #     ["key4",{"field"=>"value"}],
    #     ["key5",{"field"=>"value"}]
    #   ],...}
    def get_last(options={})
      raise "stub"
    end
  end

  module WriteAPI
    def []=(key,fields) set(key,fields) end

    # fields must be a hash or a Monotable::Record
    # returns
    #   record !existed: {:result=>:created, :size_delta=>#, :size=>#}
    #   record existed: {:result=>:replaced, :size_delta=>#, :size=>#}
    #
    # :size_delta is the change in byte-size to the store. If :created, it will be >0. Otherwise, could be anything.
    # :size is the byte-size of the record afterward
    #
    def set(key,fields)
      raise "stub"
    end

    # returns
    #   record !existed: {:result=>:created, :size_delta=>#, :size=>#}
    #   record existed: {:result=>:updated, :size_delta=>#, :size=>#}
    #
    # :size_delta is the change in byte-size to the store. If :created, it will be >0. Otherwise, could be anything.
    # :size is the byte-size of the record afterward
    #
    # Notes:
    #   We should be able to able to actually return a list of the fields that were replaced.
    #   We have all the info we need, and since we have to track accounting_size, we will always have to fetch this info to perform update.
    def update(key,fields)
      raise "stub"
    end

    # returns
    #   record existed: {:result=>:deleted, :size_delta=>#}
    #   record !existed: {:result=>:noop, :size_delta=>#}
    #
    # :size_delta is the change in byte-size to the store. It will be <0 or 0.
    def delete(key)
      raise "stub"
    end
  end

# See the ReadAPI and WriteAPI for details on the API
module API
  include ReadAPI
  include WriteAPI
end
end
