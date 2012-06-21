# https://github.com/archiloque/rest-client
# http://rubydoc.info/gems/rest-client/1.6.7/frames
# https://github.com/igrigorik/em-http-request/wiki/Issuing-Requests
module Monotable
module RestClientHelper
  private

  # return all values for "keys" in h as a hash
  def hash_select(h,keys)
    ret={}
    keys.each {|k|ret[k]=h[k] if h[k]}
    ret
  end

  # return the first key-value pair that exists from the list of "keys" in "h"
  def hash_first(h,keys)
    keys.each do |k|
      return k,h[k] if h[k]
    end
  end

  # return the uri,params for a given get-first request
  def prepare_get_first_request(options)
    kind,key = hash_first options, [:gte,:gt,:with_prefix]
    uri="first_records/#{kind}/#{ue key}"
    params = hash_select options, [:limit,:lte,:lt]
    return uri,params
  end

  # return the uri,params for a given get-last request
  def prepare_get_last_request(options)
    kind,key = hash_first options, [:lte,:lt,:with_prefix]
    uri="last_records/#{kind}/#{ue key}"
    params = hash_select options, [:limit,:gte,:gt]
    return uri,params
  end

  public

  # see ReadAPI
  def get_record(key)
    fields = self[key]
    fields && MemoryRecord.new.init(key,fields)
  end

  def process_response(code,body,options)
    if code == 200 || (options[:accept_404] && code == 404)
      if options[:raw_response]
        body
      else
        Tools.indifferentize(Tools.force_encoding(JSON.parse(body),options[:force_encoding]))
#         Tools.force_encoding(symbolize_keys(JSON.parse(body),options[:keys_to_symbolize_values]),options[:force_encoding])
      end
    elsif code==409 #&&
      (parse=JSON.parse(body)) &&
      key=parse["not_authoritative_for_key"]
      raise NotAuthoritativeForKey.new(key)
    else
      raise NetworkError.new("invalid response code: #{code.inspect} body: #{JSON.parse(body).inspect rescue body.inspect}")
    end
  end

  def em_synchrony_request(method,request_path,options={})
    request_uri = "http://"+request_path
    request = EM::HttpRequest.new(request_uri).send(
      method,
      :body => options[:body],
      :query => options[:params],
      :head => {
        :accept => "application/json",
        :content_type => options[:content_type] || "application/json",
      }
    )
    code = request.response_header.status
    process_response(code,request.response,options)
  end

  def rest_client_request(method,request_path,options={})
    RestClient::Request.execute(
      :method => method,
      :url => request_path,
      :payload => options[:body],
      :headers => {
        :params => options[:params],
        :accept => :json,
        :content_type => options[:content_type] || :json,
      }
      ) do |response, request, result|
      process_response(response.code,response.body,options)
    end
  end

  #request is the URI
  # options:
  #   :accept_404 => if true, treat 404 asif they were 200 messages (the assumption is the body encodes information about the 404 error)
  #   :params => request params
  #   :force_encoding => ruby string encoding type: Ex. "ASCII-8BIT"
  # if you include a block, this uses async_reqeust
  # for examples of using RestClient::Request.execute
  #   https://github.com/archiloque/rest-client/blob/master/lib/restclient.rb
  def request(method,request_path,options={})
    request_path = "#{server}/#{request_path}"
    if ServerClient.use_synchrony
      em_synchrony_request method, request_path, options
    else
      rest_client_request(method,request_path,options)
    end
  end
end
end
