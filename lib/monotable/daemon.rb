require 'yaml'
require 'cgi'
require 'fileutils'
require 'json'
require 'rubygems'
require 'eventmachine'
require 'evma_httpserver'
require 'uri'

class Monotable::Daemon < EM::Connection
  include EM::HttpServer
  
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
    when /^\/records(?:\/?(.*))$/
      handle_record_request($1)
    else
      handle_default_request
    end
  end
  

  def handle_record_request(key)
    
    req_call = if key.length == 0 && @http_request_method == 'GET'
      # List records
      @response.status = '200'
      @response.content_type 'text/html'
      @response.content = 'List of records'
      @response.send_response
      nil
    # elsif key.length == 0 && @http_request_method == 'POST'
    #   # Create record
    #   params = CGI.parse(@http_query_string)
    #   [:create, key, params]
    elsif key.length > 0 && @http_request_method == 'PUT'
      # Update record
      params = Hash[URI.decode_www_form(@http_post_content)]
      [:update, key, params]
    elsif key.length > 0 && @http_request_method == 'GET'
      # Read record
      [:read, key]
    elsif key.length > 0 && @http_request_method == 'DELETE'
      # Delete record
      nil
    end
    
    if req_call
      rd = RecordDeferrable.new(@response)
      rd.send(*req_call)
    else
      # Unknown request
      @response.status = 406
      @response.content_type 'text/html'
      @response.content = 'Unknown request'
      @response.send_response
    end
  end
  
  def handle_default_request
    @response.status = 200
    @response.content_type 'text/html'
    @response.content = 'Monotable'
    @response.send_response
    
  end
  
  
end

