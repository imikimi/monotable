# encoding: BINARY
module Monotable
  # see ReadAPI
  module ServerClientReadAPI
    include ReadAPI

    private
    def generate_get_range_request(first_last,options)
      [:gte,:gt,:lte,:lt,:with_prefix].each do |kind|
        if key=options[kind]
          return "#{server}/#{first_last}_records/#{kind}/#{key}"
        end
      end
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
      request=generate_get_range_request(:first,options)
      limit_options = {:limit => options[:limit]} if options[:limit]
      RestClient.get(request, :params=>limit_options, :accept=>:json) do |response, request, result|
        return Tools.force_encoding(symbolize_keys(JSON.parse(response.body)),"ASCII-8BIT") if response.code == 200 || response.code == 404
        raise NetworkError.new("invalid response code: #{response.code.inspect} for GET request: #{request.inspect}. Result: #{result.inspect}")
      end
    end

    # see ReadAPI
    def get_last(options={})
      request=generate_get_range_request(:last,options)
      limit_options = {:limit => options[:limit]} if options[:limit]
      RestClient.get(request, :params=>limit_options, :accept=>:json) do |response, request, result|
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

  class ServerClient
    include ServerClientReadAPI
    include ServerClientWriteAPI
    attr_accessor :server

    def symbolize_keys(hash,keys_to_symbolize_values={})
      ret={}
      hash.each do |k,v|
        k=k.to_sym
        v=v.to_sym if keys_to_symbolize_values[k]
        ret[k]=v
      end
      ret
    end

    def initialize(server)
      @server=server
    end

    def to_s
      @server
    end
  end
end
