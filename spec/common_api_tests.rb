def api_tests
  it "should initialize as a blank store" do
    blank_store.should_not==nil
  end
  #*******************************************************
  # test []
  #*******************************************************
  it "should get via []" do
    store=setup_store_with_test_keys
    store["key1"].should=={"data"=>"key1"}
  end

  #*******************************************************
  # test get
  #*******************************************************
  it "should get existing records" do
    store=setup_store_with_test_keys
    store.get("key1").should=={:record=>{"data"=>"key1"}, :size=>12, :num_fields=>1}
  end

  it "should get missing records" do
    store=setup_store_with_test_keys
    store.get("missing").should=={:record=>nil}
  end

  #*******************************************************
  # test get_record
  #*******************************************************
  it "should get_record existing" do
    store=setup_store_with_test_keys
    store.get_record("key1").kind_of?(Monotable::Record).should==true
  end

  it "should get_record missing" do
    store=setup_store_with_test_keys
    store.get_record("missing").kind_of?(NilClass).should==true
  end

  #*******************************************************
  # test []=
  #*******************************************************
  it "should set via []=" do
    store=blank_store #Monotable::MemoryChunk.new
    a=store["key1"]={"field1" => "value1"}; store["key1"].should==a
    a=store["key1"]={"field2" => "value2"}; store["key1"].should==a
  end

  #*******************************************************
  # test set and update
  #*******************************************************
  it "should set" do
    store=blank_store #Monotable::MemoryChunk.new
    store.set("key1", {"field1" => "value1"}).should=={:result=>:created, :size_delta=>16, :size=>16}
    store.set("key1", {"field2" => "value2"}).should=={:result=>:replaced, :size_delta=>0, :size=>16}
    store.get("key1").should=={:record=>{"field2" => "value2"},:size=>16,:num_fields=>1}
    store.set("key1", {"field2" => "vv"}).should=={:result=>:replaced, :size_delta=>-4, :size=>12}
  end


  it "should update" do
    store=blank_store #Monotable::MemoryChunk.new
    store.update("key1", {"field1" => "value1"}).should=={:result=>:created, :size_delta=>16, :size=>16}
    store.update("key1", {"field2" => "value2"}).should=={:result=>:updated, :size_delta=>12, :size=>28}
    store.get("key1").should=={:record=>{"field1" => "value1", "field2" => "value2"},:size=>28,:num_fields=>2}
    store.update("key1", {"field2" => "v"}).should=={:result=>:updated, :size_delta=>-5, :size=>23}
  end

  #*******************************************************
  # test delete
  #*******************************************************
  it "should get_record existing" do
    store=setup_store_with_test_keys
    store.get_record("key1").should_not==nil
    store.delete("key1").should=={:result=>:deleted, :size_delta=>-12}
    store.get_record("key1").should==nil
    store.delete("key1").should=={:result=>:noop, :size_delta=>0}
  end


  #*******************************************************
  # test get_first and get_last
  #*******************************************************

  it "should work to get_first :gte" do
    result=setup_store_with_test_keys.get_first(:gte=>"key2")
    result[:records].collect{|a|a[0]}.should == ["key2"]
  end

  it "should work to get_first :gt" do
    result=setup_store_with_test_keys.get_first(:gt=>"key2")
    result[:records].collect{|a|a[0]}.should == ["key3"]
  end

  it "should work to get_first :with_prefix" do
    store=setup_store_with_test_keys
    add_test_keys(store,"apple",3)
    add_test_keys(store,"legos",3)
    add_test_keys(store,"zoo",3)

    result=store.get_first(:with_prefix=>"legos", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["legos0","legos1"]
  end

  it "should work to get_first with limits" do
    store=setup_store_with_test_keys
    result=store.get_first(:gte=>"key2", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["key2","key3"]

    result=store.get_first(:gte=>"key2", :limit=>3)
    result[:records].collect{|a|a[0]}.should == ["key2","key3","key4"]

    result=store.get_first(:gte=>"key2", :limit=>4)
    result[:records].collect{|a|a[0]}.should == ["key2","key3","key4"]
  end

  it "should work to get_last :lte" do
    result=setup_store_with_test_keys.get_last(:lte=>"key2")
    result[:records].collect{|a|a[0]}.should == ["key2"]
  end

  it "should work to get_last :lt" do
    result=setup_store_with_test_keys.get_last(:lt=>"key2")
    result[:records].collect{|a|a[0]}.should == ["key1"]
  end

  it "should work to get_last :lte, :gte" do
    result=setup_store_with_test_keys.get_last(:gte => "key1", :lte=>"key3", :limit=>10)
    result[:records].collect{|a|a[0]}.should == ["key1","key2","key3"]
  end

  it "should work to get_last :lte, :gte, :limit=>2" do
    result=setup_store_with_test_keys.get_last(:gte => "key1", :lte=>"key3", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["key2","key3"]
  end

  it "should work to get_first :lte, :gte" do
    result=setup_store_with_test_keys.get_first(:gte => "key1", :lte=>"key3", :limit=>10)
    result[:records].collect{|a|a[0]}.should == ["key1","key2","key3"]
  end

  it "should work to get_first :lte, :gte, :limit=>2" do
    result=setup_store_with_test_keys.get_first(:gte => "key1", :lte=>"key3", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["key1","key2"]
  end

  it "should work to get_last with limits" do
    store=setup_store_with_test_keys
    result=store.get_last(:lte=>"key2",:limit => 2)
    result[:records].collect{|a|a[0]}.should == ["key1","key2"]

    result=store.get_last(:lte=>"key2",:limit => 3)
    result[:records].collect{|a|a[0]}.should == ["key0","key1","key2"]

    result=store.get_last(:lte=>"key2",:limit => 4)
    result[:records].collect{|a|a[0]}.should == ["key0","key1","key2"]
  end

  it "should work to get_last :with_prefix" do
    store=setup_store_with_test_keys
    add_test_keys(store,"apple",3)
    add_test_keys(store,"legos",3)
    add_test_keys(store,"zoo",3)

    result=store.get_last(:with_prefix=>"legos", :limit=>2)
    result[:records].collect{|a|a[0]}.should == ["legos1","legos2"]
  end
end
