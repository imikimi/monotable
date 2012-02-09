module Monotable
module EventMachineServer

class HttpServer < EM::Connection
  include EM::HttpServer

  class << self
    attr_reader :server

    # options
    #   :store_paths=>["path",...]
    #   :port => Fixnum - TCP port to listen on, default 8080
    #   :host => host to listen on - default "localhost"
    #   :cluster => {} => params for initializing the cluster-manager
    def start(options={})
      @server = Monotable::Server.new(options)

      if server.verbose
        puts({"Cluster status" => server.cluster_manager.status}.to_yaml)
        puts "\nMonotable init successful."
        puts "Monotable now listenting on: #{server.host}:#{server.port}"
      end

      EM.run do
        EM.threadpool_size = 1 # TODO Increase then when the localstore is thread-safe
        EM.start_server server.host, server.port,  Monotable::EventMachineServer::HttpServer
      end
    end
  end

  def server
    Monotable::EventMachineServer::HttpServer.server
  end

  def post_init
    super
    no_environment_strings
  end

  SERVER_REQUEST_PATTERN = /^\/server\/(.*)$/
  RECORDS_REQUEST_PATTERN = /^(\/internal)?\/(first_|last_|)records(.*)$/
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

    request_options = {
      :response => @response,
      :server => server,
      :params => params,
      :method => @http_request_method,
      :uri => request_uri,
      :post_content => post_content
    }

    case request_uri
    when RECORDS_REQUEST_PATTERN        then HTTP::RecordRequestHandler.new(request_options).handle
    when SERVER_REQUEST_PATTERN         then HTTP::ServerController.new(request_options).handle
    when ROOT_REQUEST_PATTERN           then handle_default_request
    else                                     handle_invalid_request("invalid URL: #{request_uri.inspect}")
    end
    if server.verbose
      puts "#{@http_request_method}:#{@http_request_uri.inspect} params: #{params.inspect}"
      puts "  post_content: #{post_content.inspect}"
      puts "  response_content: #{@response.content.inspect}"
    end
  rescue Exception => e
    puts "#{self.class} Request Error: #{e.inspect}"
    puts "    "+e.backtrace.join("    \n")
  end

  # parse the post_content
  def post_content
    @post_content=if @http_post_content && headers_hash['Content-Type'] == 'application/json'
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
