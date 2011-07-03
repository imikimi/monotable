module MonoTable
  class IndexBlockEncoder
    attr_accessor :last_key
    attr_accessor :index_records
    attr_accessor :current_block_key
    attr_accessor :current_block_offset
    attr_accessor :current_block_length
    attr_accessor :max_index_block_size
    attr_accessor :parent_index_block_encoder
    attr_accessor :total_accounting_size

    def initialize(max_index_block_size=MAX_INDEX_BLOCK_SIZE)
      @current_block_key=@last_key=""
      @current_block_offset=0
      @index_records=[]
      @current_block_length=0
      @max_index_block_size=max_index_block_size
      @total_accounting_size=0
    end

    def auto_parent_index_block_encoder
      @parent_index_block_encoder ||= IndexBlockEncoder.new(max_index_block_size)
    end

    def to_s
      # TODO: join-in the parent_index_block_encoder and the entier index's pre-block, as described a the top of this file
      # then we need to ensure the decoder can read this new format. The nice thing is the bottom-most index-level is identical to the current
      # format. So we can cheat for the first version - just skip to the bottom-most index-level

      all_index_levels=[]
      ibe=self
      while(ibe)
        all_index_levels<<ibe.index_records.join
        ibe=ibe.parent_index_block_encoder
      end
      all_index_levels.reverse!
      [
      all_index_levels.length.to_asi,
      all_index_levels.collect {|ilevel| ilevel.length.to_asi},
      all_index_levels
      ].flatten.join.to_asi_string
    end

    def add(key,offset,length,accounting_size)
      prefix_length = Tools.longest_common_prefix(key,@last_key)

      # encode record
#      encoded_index_record = IndexRecord.new.init(key,offset,length,accounting_size).to_binary(@last_key)
      encoded_index_record = DiskRecord.new.init(key,offset,length,accounting_size).encode_index_record(@last_key)

      # detect full block
      advance_block if current_block_length + encoded_index_record.length > max_index_block_size

      # add encoded record
      @total_accounting_size+=accounting_size
      @current_block_length += encoded_index_record.length
      @index_records << encoded_index_record
      @last_key=key
    end

    private

    # advanced to the next index-block
    def advance_block
      auto_parent_index_block_encoder.add(current_block_key,current_block_offset,current_block_length,@total_accounting_size)

      @current_block_key=@last_key
      @current_block_offset+=@current_block_length
      @current_block_length=0
      @total_accounting_size=0
    end
  end
end
