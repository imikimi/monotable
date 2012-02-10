module Monotable
module HttpServer
module Routes
  SERVER_REQUEST_PATTERN = /^\/server\/(.*)$/
  RECORDS_REQUEST_PATTERN = /^(\/internal)?\/(first_|last_|)records(.*)$/
  ROOT_REQUEST_PATTERN = /^\/?$/

  def uri
    @uri ||= request_options[:uri]
  end

  def body
    @body ||= request_options[:body]
  end

  def http_request_method
    @http_request_method ||= request_options[:method]
  end

  def response
    @response ||= request_options[:response]
  end
  def params
    @params ||= request_options[:params]
  end

  def route_http_request
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

    case uri
    when RECORDS_REQUEST_PATTERN        then HttpServer::RecordRequestHandler.new(request_options).handle
    when SERVER_REQUEST_PATTERN         then HttpServer::ServerController.new(request_options).handle
    when ROOT_REQUEST_PATTERN           then HttpServer::RequestHandler.new(request_options).handle_default_request
    else                                     HttpServer::RequestHandler.new(request_options).handle_invalid_request("invalid URL: #{uri.inspect}")
    end
    if request_options[:server].verbose
      puts "#{@http_request_method}:#{uri.inspect} params: #{params.inspect}"
      puts "  body: #{body.inspect}"
      puts "  response_content: #{request_options[:response].content.inspect}"
    end
  rescue Exception => e
    puts "#{self.class} Request Error: #{e.inspect}"
    puts "    "+e.backtrace.join("    \n")
  end
end
end
end
