module MonotableHelperMethods

  def add_test_keys(store,prefix,num_records)
    num_records.times do |i|
      k="#{prefix}#{i}"
      store.set(k,{"data"=>k})
    end
    store
  end

  def setup_store_with_test_keys(num_records=5)
    add_test_keys(setup_store,"key",num_records)
  end

  def load_test_data(filename)
    File.open(File.join(File.dirname(__FILE__),"test_data",filename)) {|f| return f.read.force_encoding("BINARY")}
    nil
  end

  def test_dir
    File.dirname(__FILE__)
  end

  def test_data_dir
    File.join(test_dir,"test_data")
  end

  def temp_dir
    @temp_dir||=File.join(test_dir,"tmp")
  end

  def reset_temp_dir
    Monotable::Global.reset
    `rm -rf #{temp_dir}/*`
    `mkdir -p #{temp_dir}`
    temp_dir
  end

  def load_test_data_directory(target=nil,key_prefix=nil)
    Monotable::Tools.load_directory(test_data_dir,target,key_prefix)
  end

  def chunkify_test_data_directory
    temp_dir=reset_temp_dir

    out_file=File.join(temp_dir,"test_data")

    Monotable::Tools.chunkify_directory(test_data_dir,out_file)
  end
end

class MonotableHelper
  include MonotableHelperMethods
end
