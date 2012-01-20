# encoding: BINARY
module Monotable
  module RestClientHelper
    #request is the URI
    def json_get(request_path,params={})
      request = "#{server}/#{request_path}"
      RestClient.get(request, :params=>params, :accept=>:json) do |response, request, result|
        return symbolize_keys(JSON.parse(response.body)) if response.code == 200
        raise NetworkError.new("invalid response code: #{response.code.inspect} for GET request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # payload can be a hash or a string
    def json_put(request_path,payload={})
      request = "#{server}/#{request_path}"
      RestClient.put(request, payload, :accept=>:json) do |response, request, result|
        return symbolize_keys(JSON.parse(response.body)) if response.code == 200
        raise NetworkError.new("invalid response code: #{response.code.inspect} for PUT request: #{request.inspect}. Result: #{result.inspect}")
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
      uri="#{server}/first_records/#{kind}/#{key}"
      params = hash_select options, [:limit,:lte,:lt]
      return uri,params
    end

    # return the uri,params for a given get-last request
    def prepare_get_last_request(options)
      kind,key = hash_first options, [:lte,:lt,:with_prefix]
      uri="#{server}/last_records/#{kind}/#{key}"
      params = hash_select options, [:limit,:gte,:gt]
      return uri,params
    end
    public

    # see ReadAPI
    def get_record(key)
      fields = self[key]
      fields && MemoryRecord.new.init(key,fields)
    end

    # see ReadAPI
    def get(key,field_names=nil)
      request="#{server}/records/#{key}"
      RestClient.get(request, :accept=>:json) do |response, request, result|
        return Tools.force_encoding(symbolize_keys(JSON.parse(response.body)),"ASCII-8BIT") if response.code == 200 || response.code == 404
        raise NetworkError.new("invalid response code: #{response.code.inspect} for GET request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # see ReadAPI
    def get_first(options={})
      request,params = prepare_get_first_request(options)
      RestClient.get(request, :params=>params, :accept=>:json) do |response, request, result|
        return Tools.force_encoding(symbolize_keys(JSON.parse(response.body)),"ASCII-8BIT") if response.code == 200 || response.code == 404
        raise NetworkError.new("invalid response code: #{response.code.inspect} for GET request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # see ReadAPI
    def get_last(options={})
      request,params = prepare_get_last_request(options)
      RestClient.get(request, :params=>params, :accept=>:json) do |response, request, result|
        return Tools.force_encoding(symbolize_keys(JSON.parse(response.body)),"ASCII-8BIT") if response.code == 200 || response.code == 404
        raise NetworkError.new("invalid response code: #{response.code.inspect} for GET request: #{request.inspect}. Result: #{result.inspect}")
      end
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

  # The ServerAPI object is instantiated rather than included so that it is crystal clear
  # when you are using it as these calls can be potentially unsafe if not used correctly.
  module ServerClientServerAPI
    def chunks; json_get("server/chunks")[:chunks]; end
    def up?; json_get("server/heartbeat")[:status]=="alive"; end
    def servers; json_get("server/servers")[:servers]; end
    def chunk(id); json_get("server/chunk/#{id}"); end
    def local_store_status; json_get("server/local_store_status"); end

    def join(server); json_put("server/join?server_name=#{server}"); end
  end

  class ServerClient
    include ServerClientReadAPI
    include ServerClientWriteAPI
    include ServerClientServerAPI
    include RestClientHelper
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

    def <=>(b) name<=>b.server; end
    include Comparable;

    def to_hash
      {:server_address => server}
    end
  end
end
