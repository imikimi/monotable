require 'yaml'
require 'cgi'
require 'fileutils'
require 'json'
require 'rubygems'
require 'eventmachine'
require 'evma_httpserver'
require 'uri'

module Monotable
module Daemon

class Server < EM::Connection
  include EM::HttpServer

  class <<self
    attr_accessor :local_store,:port,:host,:router

    # options
    #   :store_paths=>["path",...]
    #   :port => Fixnum - TCP port to listen on, default 8080
    #   :host => host to listen on - default "localhost"
    def start(options={})

      puts "Initializing LocalStore. Stores:\n\t#{options[:store_paths].join("\n\t")}" if options[:verbose]
      @local_store = Monotable::LocalStore.new(options)
      @router=Monotable::Router.new :local_store=>@local_store

      @port = options[:port] || 8080
      @host = options[:host] || 'localhost'

      puts "Starting Monotable on: #{@host}:#{@port}" if options[:verbose]


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

    case @http_request_uri
    when /^\/records(?:\/?(.*))$/ then  handle_record_request($1)
    else                                handle_default_request
    end
  end

  def handle_record_request(key)
    request_router=Monotable::ExternalRequestRouter.new(Server.router)
    req_call = case @http_request_method
    when 'GET'    then HTTP::RecordRequestHandler.new(@response,:store=>request_router).get(key)
    when 'POST'   then HTTP::RecordRequestHandler.new(@response,:store=>request_router).set(key,params_from_request)
    when 'PUT'    then HTTP::RecordRequestHandler.new(@response,:store=>request_router).update(key,params_from_request)
    when 'DELETE' then HTTP::RecordRequestHandler.new(@response,:store=>request_router).delete(key)
    else handle_unknown_request
    end
  end

  def handle_unknown_request
    @response.status = 406
    @response.content_type 'text/html'
    @response.content = 'Unknown request'
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

  # Extract the params from the request, based upon the mime type and request method
  def params_from_request
    if headers_hash['Accept'] == 'application/json'
      deep_to_s(JSON.parse(@http_post_content))
    end

    # Hash[URI.decode_www_form(@http_post_content)]
    # {'apple' => '1', 'banana' => '2'}
  end

  # Returns a hash of strings for the http headers
  def headers_hash
    @headers_hash ||= Hash[@http_headers.split("\x00").map{|x| x.split(': ',2)}]
  end
end
end
end
