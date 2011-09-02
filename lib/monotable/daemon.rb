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
    req_call = case @http_request_method
      when 'GET'
        # List or Read, depending on if there is a key specified
        key.length > 0 ? [:read, key] : [:list]
      when 'POST'
        # Create record
        [:create, key, params_from_request] if key.length == 0
      when 'PUT'
        # Update record
        [:update, key, params_from_request] if key.length > 0        
      when 'DELETE'
        # Delete record
        [:delete, key] if key.length > 0        
    end
            
    if req_call && req_call.any?
      rd = RecordDeferrable.new(@response)
      rd.send(*req_call)
    elsif !req_call
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

