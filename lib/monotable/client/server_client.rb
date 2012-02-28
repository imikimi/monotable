# encoding: BINARY
require 'uri'

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

    def process_response(body,options)
      if options[:raw_response]
        body
      else
        Tools.force_encoding(
          symbolize_keys(
            JSON.parse(body),
            options[:keys_to_symbolize_values]
          ),
          options[:force_encoding]
        )
      end
    end

    # Async calls return immediately.
    # When the request complets later, either:
    #   a) execute the passed in block on succcess with the response information passed in OR
    #   b) execute the optional :on_error Proc. If none, the error is ignored (TODO: log the error)
    # options
    #   :body => string or Hash (payload)
    #   :on_error => called on-error. Parameters: |http_request,error_message|
    #   :keys_to_symbolize_values => {} passed into symbolize_keys
    # block => called on success with the json-parsed response
    def em_async_request(method,request_path,options={},&block)
      headers = {:accept => :json}
      on_error = options[:on_error] || Proc.new {|http_request,message| puts message;}
      async_method = "a#{method}".to_sym

      http_request = EventMachine::HttpRequest.new(request_path).send async_method, :body => options[:body], :params=> options[:params], :head => headers

      http_request.errback { on_error.call(http_request,"request: #{method}|#{request_path}. errback called.") }
      http_request.callback do
        if http_request.response_header.status == 200 || (options[:accept_404] && http_request.response_header.status == 404)
          block.call process_response(http_request.response,options)
        else
          # error
          on_error.call(http_request,"request: #{method}|#{request_path}. invalid status code: #{http_request.response_header.status}")
        end
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
      if code == 200 || (options[:accept_404] && code == 404)
        return process_response(request.response,options)
      end
      raise NetworkError.new("invalid response code: #{code.inspect} for #{method} request: #{request.inspect}. Result: #{request.response}")
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
        if response.code == 200 || (options[:accept_404] && response.code == 404)
          return process_response(response.body,options)
        end
        raise NetworkError.new("invalid response code: #{response.code.inspect} for #{method} request: #{request.inspect}. Result: #{result.inspect}")
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
    def request(method,request_path,options={},&block)
      request_path = "#{server}/#{request_path}"
      return em_async_request method, request_path, options, &block if block
      return em_synchrony_request method, request_path, options if ServerClient.use_synchrony
      rest_client_request(method,request_path,options)
    end

    def symbolize_keys(hash,keys_to_symbolize_values=nil)
      keys_to_symbolize_values||={}
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

    # convert the record data return to Monotable::MemoryRecords
    def objectify_records(result)
      result[:records] = result[:records].collect do |rec|
        MemoryRecord.new.init(rec[0], rec[1])
      end
      result
    end

    # see ReadAPI
    def get(key,options={},&block)
      request(:get, "#{path_prefix}records/#{ue key}", :accept_404=>true, :force_encoding => "ASCII-8BIT", &block)
    end

    # return the raw value of one field as the body
    def get_field(key,field)
      request :get, "#{path_prefix}records/#{ue key}?field=#{ue field}", :accept_404=>true, :force_encoding => "ASCII-8BIT", :raw_response => true
    end

    # see ReadAPI
    def get_first(options={},&block)
      request, params = prepare_get_first_request(options)
      objectify_records request(:get, path_prefix+request, :params => params, :accept_404=>true, :force_encoding => "ASCII-8BIT", &block)
    end

    # see ReadAPI
    def get_last(options={},&block)
      request,params = prepare_get_last_request(options)
      objectify_records request(:get, path_prefix+request, :params => params, :accept_404=>true, :force_encoding => "ASCII-8BIT", &block)
    end
  end

  # see WriteAPI
  module ServerClientWriteAPI
    include WriteAPI

    # this lists the KEYS of the returned JSON for which we want to automatically SYMBOLIZE their values.
    KEYS_TO_SYMBOLIZE_VALUES={:result => true}

    # update_field exists to present an efficient way to write larger binary data over HTTP.
    def update_field(key,field,value)
      request :put, "#{path_prefix}records/#{ue key}?field=#{ue field}",
        :body => value,
        :keys_to_symbolize_values => KEYS_TO_SYMBOLIZE_VALUES,
        :content_type => "application/octet-stream"
    end

    # see WriteAPI
    def set(key,fields,&block)
      fields = Tools.force_encoding(fields,"UTF-8")
      request :post, "#{path_prefix}records/#{ue key}", :body => fields.to_json, :keys_to_symbolize_values => KEYS_TO_SYMBOLIZE_VALUES, &block
    end

    # see WriteAPI
    def update(key,fields,&block)
      fields = Tools.force_encoding(fields,"UTF-8")
      request :put, "#{path_prefix}records/#{ue key}", :body => fields.to_json, :keys_to_symbolize_values => KEYS_TO_SYMBOLIZE_VALUES, &block
    end

    # see WriteAPI
    def delete(key,&block)
      request :delete, "#{path_prefix}records/#{ue key}", :keys_to_symbolize_values => KEYS_TO_SYMBOLIZE_VALUES, &block
    end
  end

  # this api is supported for testing, it should never be used by an actual client
  module ServerClientServerReadAPI
    def chunks; request(:get,"server/chunks")[:chunks]; end
    def servers; request(:get,"server/servers")[:servers]; end

    # returns nil if chunk not found
    def chunk(key); request(:get,"server/chunk/#{ue key}", :accept_404=>true)[:chunk_info]; end

    # returns nil if chunk not found
    def chunk_keys(key); request(:get,"server/chunk_keys/#{ue key}", :accept_404=>true)[:keys]; end

    #
    def local_store_status; request(:get,"server/local_store_status"); end

    # returns true if the server is up and responding to the heartbeat
    def up?
      request(:get,"server/heartbeat")[:status]=="alive";
    rescue Errno::ECONNREFUSED => e
    end
  end

  # this api is supported for testing, it should never be used by an actual client
  module ServerClientServerModifyAPI
    def split_chunk(on_key); request(:post,"server/split_chunk/#{ue on_key}")[:chunks]; end
    def balance; request(:post,"server/balance"); end
    def join(server,skip_servers=[]); request(:put,"server/join?server_name=#{ue server}&skip_servers=#{ue skip_servers.join(',')}")[:servers]; end
    def update_servers(servers,skip_servers=[]); request(:post,"server/update_servers?servers=#{ue servers.join(',')}&skip_servers=#{ue skip_servers.join(',')}")[:servers]; end

    # returns the raw chunk-file
    def up_replicate_chunk(chunk_key); request(:post,"server/up_replicate_chunk/#{ue chunk_key}",:raw_response => true); end
    def down_replicate_chunk(chunk_key); request(:post,"server/down_replicate_chunk/#{ue chunk_key}"); end
  end

  class ServerClient
    include ServerClientReadAPI
    include ServerClientWriteAPI
    include ServerClientServerReadAPI
    include ServerClientServerModifyAPI
    include RestClientHelper
    attr_accessor :server, :client_options
    attr_accessor :path_prefix
    class <<self
      attr_accessor :use_synchrony
    end

    # uri-encode string
    def ue(str)
      URI.encode(str)
    end

    #options
    def initialize(server,options={})
      @server=server
      @client_options = options
      @path_prefix = options[:internal] ? "internal/" : ""
    end

    # returns an internal server client
    def internal
      @internal||=ServerClient.new(server,client_options.merge(:internal => true))
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
