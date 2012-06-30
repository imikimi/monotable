module Monotable
module HttpServer
module Routes
  SERVER_REQUEST_PATTERN =  %r{^/server/(.*)$}
  RECORDS_REQUEST_PATTERN = %r{^(/internal)?/(first_|last_|)records(.*)$}
  ROOT_REQUEST_PATTERN =    %r{^/?$}

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

  def server; @server ||= request_options[:server]; end

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

#    puts "<PROCESSING uri=#{uri.inspect}>"
    case uri
    when RECORDS_REQUEST_PATTERN        then HttpServer::RecordRequestHandler.new(request_options).handle
    when SERVER_REQUEST_PATTERN         then HttpServer::ServerController.new(request_options).handle
    when ROOT_REQUEST_PATTERN           then HttpServer::RequestHandler.new(request_options).handle_default_request
    else                                     HttpServer::RequestHandler.new(request_options).handle_invalid_request("invalid URL: #{uri.inspect}")
    end
#    puts "</PROCESSING uri=#{uri.inspect}>"
    if request_options[:server].verbose
      puts "#{request_options[:method]}:#{uri.inspect} params: #{params.inspect}"
      puts "  body: #{body.inspect}"
      puts "  response_content: #{request_options[:response].content.inspect}"
    end
  rescue Monotable::ArgumentError => e
    # this is a non-error case for the server; the client sent an invalid request
    respond(400, :error => e.inspect)
  rescue Exception => e
    puts "#{self.class}#route_http_request Internal Error: #{e.inspect}"
    puts "  "+e.backtrace.join("\n  ")
    respond(500, :error => e.to_s)
  end

  def respond(status,content)
    HttpServer::RequestHandler.new(request_options).respond(status,content)
  end
end
end
end
