# encoding: BINARY
module Monotable

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
    def get(key,options={})
      request(:get, "#{path_prefix}records/#{ue key}", :accept_404=>true, :force_encoding => "ASCII-8BIT")
    end

    # return the raw value of one field as the body
    def get_field(key,field)
      request :get, "#{path_prefix}records/#{ue key}?field=#{ue field}", :accept_404=>true, :force_encoding => "ASCII-8BIT", :raw_response => true
    end

    # see ReadAPI
    def get_first(options={})
      request, params = prepare_get_first_request(options)
      objectify_records request(:get, path_prefix+request, :params => params, :accept_404=>true, :force_encoding => "ASCII-8BIT")
    end

    # see ReadAPI
    def get_last(options={})
      request,params = prepare_get_last_request(options)
      objectify_records request(:get, path_prefix+request, :params => params, :accept_404=>true, :force_encoding => "ASCII-8BIT")
    end
  end

  # see WriteAPI
  module ServerClientWriteAPI
    include WriteAPI

    # update_field exists to present an efficient way to write larger binary data over HTTP.
    def update_field(key,field,value)
      request :put, "#{path_prefix}records/#{ue key}?field=#{ue field}",
        :body => value,
        :content_type => "application/octet-stream"
    end

    # see WriteAPI
    def set(key,fields)
      raise Monotable::ArgumentError.new("fields must be a Hash") unless fields.kind_of? Hash
      fields = Tools.force_encoding(fields,"UTF-8")
      request :post, "#{path_prefix}records/#{ue key}", :body => fields.to_json
    end

    # see WriteAPI
    def update(key,fields)
      fields = Tools.force_encoding(fields,"UTF-8")
      request :put, "#{path_prefix}records/#{ue key}", :body => fields.to_json
    end

    # see WriteAPI
    def delete(key)
      request :delete, "#{path_prefix}records/#{ue key}"
    end
  end


  class ServerClient
    include ServerClientReadAPI
    include ServerClientWriteAPI
    include ServerClientInternalAPI
    include RestClientHelper
    attr_accessor :server, :client_options
    attr_accessor :path_prefix
    class <<self
      attr_accessor :use_synchrony
    end

    # uri-encode string
    def ue(str)
      URI.encode(str.to_s)
    end

    #options
    def initialize(server,options={})
      @server=server
      raise "invalid server #{server.inspect}" if server[/^http/]
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
