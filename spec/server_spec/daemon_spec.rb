require File.join(File.dirname(__FILE__),"..","mono_table_helper_methods")

describe Monotable::EventMachineServer do
  include DaemonTestHelper

  before(:each) do
    start_daemon
  end

  after(:each) do
    shutdown_daemon
  end

  it "should be accessible via HTTP" do
    Net::HTTP.get(host,'/',port).should_not be_empty
  end

  it "returns 404 for a non-existent record" do
    res = Net::HTTP.get_response(host,"/records/missing", port)
    res.code.should == '404'
  end

  it "return the record we create when using JSON" do
    record_key = 'apple'
    record_value = { 'x' => '1' }
    RestClient.put("#{daemon_uri}/records/#{record_key}", record_value.to_json, :content_type => :json, :accept => :json)
    response = JSON.parse(RestClient.get("#{daemon_uri}/records/#{record_key}"))
    response["record"].should==record_value
  end

  it "should not return the record after we've deleted it" do
    record_key = 'apple'
    record_value = { 'x' => '1' }
    # First write the record, and make sure it is there
    RestClient.put("#{daemon_uri}/records/#{record_key}", record_value.to_json, :content_type => :json, :accept => :json)
    RestClient.get("#{daemon_uri}/records/#{record_key}") {|response, request, result|
      response.code.should == 200
    }
    # Then delete the record, and make sure it is not there
    RestClient.delete("#{daemon_uri}/records/#{record_key}")
    RestClient.get("#{daemon_uri}/records/#{record_key}") {|response, request, result|
      response.code.should == 404
    }
  end

  it "it should work to get_first / get_last across chunk boundaries" do
    setup_store(5)
    server_client.split_chunk("u/key3")
    server_client.chunks.should == ["","u/key3"]

    # get_first test
    res = server_client.get_first(:gte => "key2", :limit=>3)
    res[:records].collect{|a|a.to_ruby}.should==[["key2", {"field"=>"2"}]]
    res[:next_options].should=={"limit"=>2, "gte"=>"key3"}

    # get_first test "next"
    res = server_client.get_first(Monotable::Tools.indifferentize res[:next_options])
    res[:records].collect{|a|a.to_ruby}.should==[["key3", {"field"=>"3"}], ["key4", {"field"=>"4"}]]
    res[:next_options].should==nil

    # get_last test
    res = server_client.get_last(:lte => "key4", :limit=>3)
    res[:records].collect{|a|a.to_ruby}.should==[["key3", {"field"=>"3"}], ["key4", {"field"=>"4"}]]
    res[:next_options].should=={"limit"=>1, "lt"=>"key3"}

    # get_last test "next"
    res = server_client.get_last(res[:next_options])
    res[:records].collect{|a|a.to_ruby}.should==[["key2", {"field"=>"2"}]]
    res[:next_options].should==nil
  end

  it "should handle setting with non-string values gracefully" do
    setup_store 2

    server_client.set "foo", "field" => 1, 2 => "food"
    rec = server_client.get_record "foo"
    rec["field"].should == "1"
    rec["2"].should == "food"
  end

  it "should be possible to delete all records in the store" do
    setup_store(2)
    clear_store

    JSON.parse(RestClient.get("#{daemon_uri}/first_records/gte"))["records"].length.should == 0
  end

  it "should respond to /first_records/gte" do
    setup_store(9)

    JSON.parse(RestClient.get("#{daemon_uri}/first_records/gte", :params => {:limit=>1})).should>=
      {"records"=>[["key1", {'field'=>"1"}]], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/first_records/gte", :params => {:limit=>2})).should>=
      {"records"=>[["key1", {'field'=>"1"}], ["key2", {'field'=>"2"}]], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/first_records/gte/key2", :params => {:limit=>2})).should>=
      {"records"=>[["key2", {'field'=>"2"}], ["key3", {'field'=>"3"}]], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/first_records/gte/key1", :params => {:limit=>2})).should>=
      {"records"=>[["key1", {'field'=>"1"}], ["key2", {'field'=>"2"}]], "next_options"=>nil}
  end

  it "should respond to /first_records/ge" do
    setup_store(9)

    JSON.parse(RestClient.get("#{daemon_uri}/first_records/gt/key1", :params => {:limit=>2})).should>=
      {"records"=>[["key2", {'field'=>"2"}],["key3", {'field'=>"3"}]], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/first_records/gt", :params => {:limit=>2})).should>=
      {"records"=>[["key1", {'field'=>"1"}], ["key2", {'field'=>"2"}]], "next_options"=>nil}
  end

  it "should respond to /last_records/lte" do
    setup_store(9)

    JSON.parse(RestClient.get("#{daemon_uri}/last_records/lte/key0", :params => {:limit=>10})).should>=
      {"records"=>[], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/last_records/lte/key1", :params => {:limit=>10})).should>=
      {"records"=>[["key1", {'field'=>"1"}]], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/last_records/lte/key2", :params => {:limit=>10})).should>=
      {"records"=>[["key1", {'field'=>"1"}],["key2", {'field'=>"2"}]], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/last_records/lte/key3", :params => {:limit=>2})).should>=
      {"records"=>[["key2", {'field'=>"2"}],["key3", {'field'=>"3"}]], "next_options"=>nil}
  end

  it "should respond to /last_records/lt" do
    setup_store(9)

    JSON.parse(RestClient.get("#{daemon_uri}/last_records/lt/key0", :params => {:limit=>10})).should>=
      {"records"=>[], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/last_records/lt/key1", :params => {:limit=>10})).should>=
      {"records"=>[], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/last_records/lt/key2", :params => {:limit=>10})).should>=
      {"records"=>[["key1", {'field'=>"1"}]], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/last_records/lt/key5", :params => {:limit=>2})).should>=
      {"records"=>[["key3", {'field'=>"3"}],["key4", {'field'=>"4"}]], "next_options"=>nil}

    JSON.parse(RestClient.get("#{daemon_uri}/last_records/with_prefix/", :params => {:limit=>2})).should>=
      {"records"=>[["key8", {'field'=>"8"}],["key9", {'field'=>"9"}]], "next_options"=>nil}
  end

  it "should respond to 406 to invalide requests" do
    expect{RestClient.get("#{daemon_uri}/first_records/lte/key1")}.to raise_error(RestClient::NotAcceptable)
  end

end
