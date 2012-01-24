# encoding: BINARY
module Monotable

  # Async calls return immediately.
  # When the request complets later, either:
  #   a) execute the passed in block on succcess with the response information passed in OR
  #   b) execute the optional :on_error Proc. If none, the error is ignored (TODO: log the error)
  module AsyncRestClientHelper
    # options
    #   :params => request params hash
    #   :on_error => called on-error. Parameters: |http_request,error_message|
    #   :accept_404 => on 404 results, parse the body and treat it as a success - the body should transfer the information that it was a 404
    # block => called on success with the json-parsed response
    def async_json_get(request_path,options={},&block)
      headers = {:accept => :json}
      http_request = EventMachine::HttpRequest.new(request_path).get :params => options[:params], :head => headers
      on_error = options[:on_error] || Proc.new {|http_request|}

      http_request.errback { on_error.call(http,"errback") }
      http_request.callback do
        if http_request.response_header.status == 200 || (options[:accept_404] && http_request.response_header.status == 404)
          block.call(Tools.force_encoding(symbolize_keys(JSON.parse(http_request.response)),"ASCII-8BIT"))
        else
          # error
          on_error.call(http,"invalid status code: #{http_request.response_header.status}")
        end
      end
    end
  end

  module ReadClientHelper
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
      uri="first_records/#{kind}/#{key}"
      params = hash_select options, [:limit,:lte,:lt]
      return uri,params
    end

    # return the uri,params for a given get-last request
    def prepare_get_last_request(options)
      kind,key = hash_first options, [:lte,:lt,:with_prefix]
      uri="last_records/#{kind}/#{key}"
      params = hash_select options, [:limit,:gte,:gt]
      return uri,params
    end

    public

    # see ReadAPI
    def get_record(key)
      fields = self[key]
      fields && MemoryRecord.new.init(key,fields)
    end
  end

  module RestClientHelper
    #request is the URI
    # options:
    #   :accept_404 => if true, treat 404 asif they were 200 messages (the assumption is the body encodes information about the 404 error)
    #   :params => request params
    #   :force_encoding => ruby string encoding type: Ex. "ASCII-8BIT"
    def json_get(request_path,options={},&block)
      request = "#{server}/#{request_path}"
      return async_json_get request,options,&block if block
      RestClient.get(request, :params=>options[:params], :accept=>:json) do |response, request, result|
        if response.code == 200 || (options[:accept_404] && response.code == 404)
          return Tools.force_encoding(symbolize_keys(JSON.parse(response.body)),options[:force_encoding])
        end
        raise NetworkError.new("invalid response code: #{response.code.inspect} for GET request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # options
    #   :payload can be a hash or a string
    def put(request_path,options={},&block)
      request = "#{server}/#{request_path}"
      return async_put request,options,&block if block
      RestClient.put(request, options[:payload]||{}) do |response, request, result|
        return response.body if response.code == 200
        raise NetworkError.new("invalid response code: #{response.code.inspect} for PUT request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # options
    #   :payload can be a hash or a string
    def post(request_path,options={},&block)
      request = "#{server}/#{request_path}"
      return async_post request,options,&block if block
      RestClient.post(request, options[:payload]||{}) do |response, request, result|
        return response.body if response.code == 200
        raise NetworkError.new("invalid response code: #{response.code.inspect} for POST request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # options
    #   :payload can be a hash or a string
    def json_put(request_path,options={},&block)
      request = "#{server}/#{request_path}"
      return async_json_put request,options,&block if block
      RestClient.put(request, options[:payload]||{}, :accept=>:json) do |response, request, result|
        return symbolize_keys(JSON.parse(response.body)) if response.code == 200
        raise NetworkError.new("invalid response code: #{response.code.inspect} for PUT request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # options
    #   :payload can be a hash or a string
    def json_post(request_path,options={},&block)
      request = "#{server}/#{request_path}"
      return async_json_post request,options,&block if block
      RestClient.post(request, options[:payload]||{}, :accept=>:json) do |response, request, result|
        return symbolize_keys(JSON.parse(response.body)) if response.code == 200
        raise NetworkError.new("invalid response code: #{response.code.inspect} for POST request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    def symbolize_keys(hash,keys_to_symbolize_values={})
      ret={}
      hash.each do |k,v|
        k=k.to_sym
        v=v.to_sym if keys_to_symbolize_values[k]
        ret[k]=v
      end
      ret
    end
  end

  # see ReadAPI
  module ServerClientReadAPI
    include ReadAPI
    include ReadClientHelper

    # see ReadAPI
    def get(key,options={},&block)
      json_get "records/#{key}", options.merge(:accept_404=>true, :force_encoding => "ASCII-8BIT"),&block
    end

    # see ReadAPI
    def get_first(options={})
      request, params = prepare_get_first_request(options)
      json_get request, options.merge(:params => params, :accept_404=>true, :force_encoding => "ASCII-8BIT")
    end

    # see ReadAPI
    def get_last(options={})
      request,params = prepare_get_last_request(options)
      json_get request, options.merge(:params => params, :accept_404=>true, :force_encoding => "ASCII-8BIT")
    end
  end

  # see WriteAPI
  module ServerClientWriteAPI
    include WriteAPI

    KEYS_TO_SYMOBLIZE_VALUES={:result => true}

    # see WriteAPI
    def set(key,fields)
      request="#{server}/records/#{key}"
      fields = Tools.force_encoding(fields,"UTF-8")
      RestClient.post(request, fields.to_json, :content_type => :json, :accept => :json) do |response, request, result|
        return symbolize_keys(JSON.parse(response.body),KEYS_TO_SYMOBLIZE_VALUES) if response.code == 200
        raise NetworkError.new("invalid response code: #{response.code.inspect} for GET request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # see WriteAPI
    def update(key,fields)
      request="#{server}/records/#{key}"
      fields = Tools.force_encoding(fields,"UTF-8")
      RestClient.put(request, fields.to_json, :content_type => :json, :accept => :json) do |response, request, result|
        return symbolize_keys(JSON.parse(response.body),KEYS_TO_SYMOBLIZE_VALUES) if response.code == 200
        raise NetworkError.new("invalid response code: #{response.code.inspect} for GET request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # see WriteAPI
    def delete(key)
      request="#{server}/records/#{key}"
      RestClient.delete(request, :accpet=>:json) do |response, request, result|
        return symbolize_keys(JSON.parse(response.body),KEYS_TO_SYMOBLIZE_VALUES) if response.code == 200
        raise NetworkError.new("invalid response code: #{response.code.inspect} for GET request: #{request.inspect}. Result: #{result.inspect}")
      end
    end
  end

  # this api is supported for testing, it should never be used by an actual client
  module ServerClientServerReadAPI
    def chunks; json_get("server/chunks")[:chunks]; end
    def up?; json_get("server/heartbeat")[:status]=="alive"; end
    def servers; json_get("server/servers")[:servers]; end
    def chunk(id); json_get("server/chunk/#{id}"); end
    def local_store_status; json_get("server/local_store_status"); end
  end

  # this api is supported for testing, it should never be used by an actual client
  module ServerClientServerModifyAPI
    def balance; json_post("server/balance"); end
    def join(server); json_put("server/join?server_name=#{server}"); end

    # returns the raw chunk-file
    def up_replicate_chunk(chunk_key); post("server/up_replicate_chunk/#{chunk_key}"); end
    def down_replicate_chunk(chunk_key); post("server/down_replicate_chunk/#{chunk_key}"); end
  end

  class ServerClient
    include ServerClientReadAPI
    include ServerClientWriteAPI
    include ServerClientServerReadAPI
    include ServerClientServerModifyAPI
    include RestClientHelper
    include AsyncRestClientHelper
    attr_accessor :server


    def initialize(server)
      @server=server
    end

    def to_s
      @server
    end

    # when called from a parent to_json, two params are passed in; ignored here
    def to_json(a=nil,b=nil)
      to_hash.to_json
    end

    def <=>(b) server<=>b.server; end
    include Comparable;

    def to_hash
      {:server_address => server}
    end
  end
end
