=begin

SBD: I'd like to split up the api clearly into an api that deals with Record
objects and one that deals with the raw ruby structures. Further, the raw api
won't automatically perform repeat requests to fetch all the requested data
whereas the Record api will.

Further, the Record objects should support the "Save" operation - setting the
current values back to the monotable. We may want some additional features:

  save-only-changed-fields
  safe-save (save only if fields didn't change in the monotable)
  save! - just save, overwriting all with the current records

These can all be simulated by the more basic set, update, and cas operations.

  #**************************
  # raw api
  #**************************

  # returns:
  #   exists: {:record=>{hash-of-fields}, ...}
  #   !exist: {:record=>nil}
  def get_raw(key,fields={}) end

  # Fetches only the first batch of records and returns the options for the next batch in :next_options
  # Returns
  #   {:records=>[[key,{fields_hash}]], :next_options=>{...}}
  def get_first_raw(options={}) end

  # Fetches only the first batch of records and returns the options for the next batch in :next_options
  # Returns
  #   {:records=>[[key,{fields_hash}]], :next_options=>{...}}
  def get_last_raw(options={}) end

  #**************************
  # record api
  #**************************

  # Automatically makes repeated calls to fetch all requested records up to :limit
  # Returns
  #   Record
  def get(key,fields={})

  # Automatically makes repeated calls to fetch all requested records up to :limit
  # Returns
  #   [Records]
  def get_first(options={}) end

  # Returns
  #   [Records]
  def get_last(options={}) end

=end


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
      fields = get(key)[:record]
      MemoryRecord.new.init(key,fields) if fields
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
    #   {:records=>[[key,record],...],:next_options}
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
    #   {:records=>[[key,Record],[...],...], :next_options=>{...}}
    #
    # Examples
    #   store.get_first(:gte=>"key2") => {:records=>[["key2",Record.new("field"=>"value")]], ...}
    #   store.get_first(:gt=>"key2") => {:records=>[["key3",Record.new("field"=>"value")]], ...}
    #   store.get_first(:with_prefix=>"key2") => {:records=>[["key3",Record.new("field"=>"value")]], ...}
    #   store.get_first(:gt=>"key2", :limit=>3) => {:records=>[
    #     ["key3",Record.new("field"=>"value")],
    #     ["key4",Record.new("field"=>"value")],
    #     ["key5",Record.new("field"=>"value")]
    #   ],...}
    def get_last(options={})
      raise "stub"
    end
  end

  module WriteAPI
    def []=(key,fields) set(key,fields) end

    # set replaces the entire value of the record
    # fields must be a hash or a Monotable::Record
    # returns
    #   record !existed: {:result=>"created", :size_delta=>#, :size=>#}
    #   record existed: {:result=>"replaced", :size_delta=>#, :size=>#}
    #
    # :size_delta is the change in byte-size to the store. If "created", it will be >0. Otherwise, could be anything.
    # :size is the byte-size of the record afterward
    #
    def set(key,fields)
      raise "stub"
    end

    # update will only overwrite the listed fields, other fields are left unchanged
    # returns
    #   record !existed: {:result=>"created", :size_delta=>#, :size=>#}
    #   record existed: {:result=>"updated", :size_delta=>#, :size=>#}
    #
    # :size_delta is the change in byte-size to the store. If "created", it will be >0. Otherwise, could be anything.
    # :size is the byte-size of the record afterward
    #
    # Notes:
    #   We should be able to able to actually return a list of the fields that were replaced.
    #   We have all the info we need, and since we have to track accounting_size, we will always have to fetch this info to perform update.
    def update(key,fields)
      raise "stub"
    end

    # returns
    #   record existed: {:result=>"deleted", :size_delta=>#}
    #   record !existed: {:result=>"no-op", :size_delta=>#}
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
