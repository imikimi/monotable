module Monotable
module EventMachineServer

class HttpServer < EM::Connection
  include EM::HttpServer
  include Monotable::HttpServer::Routes

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

  # EventMachine call this to start the request processing
  def process_http_request
    route_http_request
  end

  def post_init
    super
    no_environment_strings
  end

  def uri
    @http_request_uri
  end

  def request_options
    @request_options ||= {
      :response => EM::DelegatedHttpResponse.new(self) ,
      :server => Monotable::EventMachineServer::HttpServer.server,
      :params => params,
      :method => @http_request_method,
      :uri => uri ,
      :post_content => post_content
    }
  end

  # parse the post_content
  def post_content
    @post_content||=if @http_post_content && headers_hash['Content-Type'] == 'application/json'
      deep_to_s(JSON.parse(@http_post_content))
    else
      {}
    end
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

  # generate the params hash
  def params
    @params||=Addressable::URI.parse("?#{@http_query_string}").query_values
  end

  # Returns a hash of strings for the http headers
  def headers_hash
    @headers_hash ||= Hash[@http_headers.split("\x00").map{|x| x.split(': ',2)}]
  end
end
end
end
