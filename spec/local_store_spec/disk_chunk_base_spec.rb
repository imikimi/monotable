require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")

describe Monotable::DiskChunkBase do
  include MonotableHelperMethods
  it_should_behave_like "monotable api"

  def blank_store
    reset_temp_dir
    filename=File.join(temp_dir,"test#{Monotable::CHUNK_EXT}")
    Monotable::MemoryChunk.new().save(filename)
    Monotable::DiskChunkBase.new(:filename=>filename)
  end

end
