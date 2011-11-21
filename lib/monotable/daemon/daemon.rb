require 'yaml'
require 'cgi'
require 'fileutils'
require 'json'
require 'rubygems'
require 'eventmachine'
require 'evma_httpserver'
require 'uri'
require "addressable/uri"

module Monotable
module Daemon

class Server < EM::Connection
  include EM::HttpServer

  class <<self
    attr_accessor :local_store,:port,:host,:router,:verbose

    # options
    #   :store_paths=>["path",...]
    #   :port => Fixnum - TCP port to listen on, default 8080
    #   :host => host to listen on - default "localhost"
    def start(options={})
      @verbose=options[:verbose]

      puts "Initializing LocalStore. Stores:\n\t#{options[:store_paths].join("\n\t")}" if verbose
      @local_store = Monotable::LocalStore.new(options)
      @router=Monotable::Router.new :local_store=>@local_store

      @port = options[:port] || 8080
      @host = options[:host] || 'localhost'

      puts "Starting Monotable on: #{@host}:#{@port}" if verbose


      EM.run do
        EM.threadpool_size = 1 # TODO Increase then when the localstore is thread-safe
        EM.start_server @host, @port,  Monotable::Daemon::Server
      end
    end
  end

  def post_init
    super
    no_environment_strings
  end

  INTERNAL_REQUEST_PATTERN = /^\/internal\/(.*)$/
  RECORDS_REQUEST_PATTERN = /^\/records(?:\/?(.*))$/
  SERVER_REQUEST_PATTERN = /^\/server\/([a-zA-Z]+)\/?(.*)$/
  FIRST_RECORDS_REQUEST_PATTERN = /^\/first_records\/(gt|gte|with_prefix)(\/(.+)?)?$/
  LAST_RECORDS_REQUEST_PATTERN = /^\/last_records\/(lt|lte|with_prefix)(\/(.+)?)?$/
  ROOT_REQUEST_PATTERN = /^\/?$/
  def process_http_request
    # the http request details are available via the following instance variables:
    #   @http_protocol
    #   @http_request_method
    #   @http_cookie
    #   @http_if_none_match
    #   @http_content_type
    #   @http_path_info
    #   @http_request_uri
    #   @http_query_string
    #   @http_post_content
    #   @http_headers

    @response = EM::DelegatedHttpResponse.new(self)

    request_uri = @http_request_uri

    request_router=nil
    if @http_request_uri[INTERNAL_REQUEST_PATTERN]
      request_uri="/#{$1}"
      request_router=Monotable::InternalRequestRouter.new(Server.router)
    end


    case request_uri
    when RECORDS_REQUEST_PATTERN        then handle_record_request(request_router,$1)
    when FIRST_RECORDS_REQUEST_PATTERN  then handle_first_records_request(request_router,params.merge($1=>$3))
    when LAST_RECORDS_REQUEST_PATTERN   then handle_last_records_request(request_router,params.merge($1=>$3))
    when SERVER_REQUEST_PATTERN         then handle_server_request($1.downcase,$2)
    when ROOT_REQUEST_PATTERN           then handle_default_request
    else                                     handle_invalid_request("invalid URL: #{request_uri.inspect}")
    end
    if Server.verbose
      puts "#{@http_request_method}:#{@http_request_uri}(#{params.inspect})"
      puts "  post_content: #{post_content.inspect}"
      puts "  response_content: #{@response.content.inspect}"
    end
  end
=begin
/first_records/gt/
/first_records/gte/
/first_records/with_prefix/
/last_records/lt/
/last_records/lte/
/last_records/with_prefix/
=end

  def handle_server_request(action,key)
    case @http_request_method
    when 'GET' then
      case action
      when 'chunks' then HTTP::InternalRequestHandler.new(@response).chunks
      else handle_unknown_request
      end
    else handle_unknown_request
    end
  end

  def handle_record_request(request_router,key)
    request_router||=Monotable::ExternalRequestRouter.new(Server.router)
    case @http_request_method
    when 'GET'    then HTTP::RecordRequestHandler.new(@response,:store=>request_router).get(key)
    when 'POST'   then HTTP::RecordRequestHandler.new(@response,:store=>request_router).set(key,post_content)
    when 'PUT'    then HTTP::RecordRequestHandler.new(@response,:store=>request_router).update(key,post_content)
    when 'DELETE' then HTTP::RecordRequestHandler.new(@response,:store=>request_router).delete(key)
    else handle_unknown_request
    end
  end

  VALID_FIRST_LAST_PARAMS=%w{ lt lte gt gte with_prefix limit fields }

  # parse the post_content
  def post_content
    @post_content=if headers_hash['Content-Type'] == 'application/json'
      deep_to_s(JSON.parse(@http_post_content))
    else
      {}
    end
  end

  # the params with the keys symbolized if all params are in the valid_params list,
  # else this sets up an invalid_request response
  def validate_params(valid_params,p=nil)
    p||=params
    count=0
    valid_params.each do |kstr|
      count+=1 if p.has_key? kstr
    end
    if count!=p.length
      handle_invalid_request "Query-string parameters must be one of: #{valid_params.inspect}"
      false
    else
      Hash[p.collect {|k,v| [k.to_sym,v]}]
    end
  end

  def handle_first_records_request(request_router,options)
    puts "handle_first_records_request options=#{options.inspect}" if Server.verbose
    return unless options=validate_params(VALID_FIRST_LAST_PARAMS,options)
    options[:limit]=options[:limit].to_i if options[:limit]
    puts "handle_first_records_request options=#{options.inspect}" if Server.verbose
    request_router||=Monotable::ExternalRequestRouter.new(Server.router)
    return handle_unknown_request unless @http_request_method=='GET'
    HTTP::RecordRequestHandler.new(@response,:store=>request_router).get_first(options)
  end

  def handle_last_records_request(request_router,options)
    puts "handle_last_records_request options=#{options.inspect}" if Server.verbose
    return unless options=validate_params(VALID_FIRST_LAST_PARAMS,options)
    options[:limit]=options[:limit].to_i if options[:limit]
    puts "handle_last_records_request options=#{options.inspect}" if Server.verbose
    request_router||=Monotable::ExternalRequestRouter.new(Server.router)
    return handle_unknown_request unless @http_request_method=='GET'
    HTTP::RecordRequestHandler.new(@response,:store=>request_router).get_first(options)
  end

  def handle_unknown_request
    @response.status = 406
    @response.content_type 'text/html'
    @response.content = 'Unknown request'
    @response.send_response
  end

  def handle_invalid_request(message)
    @response.status = 406
    @response.content_type 'text/html'
    @response.content = message || 'Invalid request'
    @response.send_response
  end

  def handle_default_request
    @response.status = 200
    @response.content_type 'text/html'
    @response.content = 'Monotable'
    @response.send_response
  end

  # Turns everything in the hash to a string
  # Does not preserve all the structure we may want; consider tweaking.
  def deep_to_s(obj)
    if obj.is_a?(Hash)
      Hash[obj.map{|k,v| [k.to_s, v.to_s]}]
    else
      obj.to_s
    end
  end

  def parse_query_string
    Addressable::URI.parse("?#{@http_query_string}").query_values
  end

  def params
    @params||=parse_query_string
  end

  # Returns a hash of strings for the http headers
  def headers_hash
    @headers_hash ||= Hash[@http_headers.split("\x00").map{|x| x.split(': ',2)}]
  end
end
end
end