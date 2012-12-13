require 'goliath'
#at_exit { exit! }
module Goliath
class Application
  def self.run!
    puts "Goliath attempting to run at exit... blocked"
  end
end
end

=begin
env parameters:

  FRAGMENT                       => ""
  HTTP_ACCEPT                    => "*/*; q=0.5, application/xml"
  HTTP_ACCEPT_ENCODING           => "gzip, deflate"
  HTTP_HOST                      => "localhost:9000"
  HTTP_USER_AGENT                => "Ruby"
  HTTP_VERSION                   => "1.1"
  PATH_INFO                      => "/"
  QUERY_STRING                   => "query=love%20potion"
  REMOTE_ADDR                    => "127.0.0.1"
  REQUEST_METHOD                 => "GET"
  REQUEST_PATH                   => "/"
  REQUEST_URI                    => "/?query=love%20potion"
  SCRIPT_NAME                    => "/"
  SERVER_NAME                    => "localhost"
  SERVER_PORT                    => "9000"
  SERVER_SOFTWARE                => "Goliath"
  async.callback                 => #<Proc:0x007fd1d2b470c0@/Users/shanebdavis/.rvm/gems/ruby-1.9.2-p290/gems/goliath-0.9.4/lib/goliath/rack/async_middleware.rb:92>
  config                         => {}
  options                        => {}
  params                         => {"query"=>"love potion"}
  rack.errors                    => (IO) #<IO:<STDERR>>
  rack.input                     => (StringIO) #<StringIO:0x007fd1d23720c0>
  rack.logger                    => (Log4r::Logger) #<Log4r::Logger:0x007fd1d241f220 @fullname="goliath", @outputters=[#<Log4r::StdoutOutputter:0x007fd1d2367fd0 @mon_owner=nil, @mon_count=0, @mon_mutex=#<Mutex:0x007fd1d2367f80>, @name="console", @level=0, @formatter=#<Log4r::PatternFormatter:0x007fd1d23613b0 @depth=7, @pattern="[9108:%l] %d :: %m", @date_pattern="%Y-%m-%d %H:%M:%S", @date_method=nil>, @out=#<IO:<STDOUT>>>], @additive=true, @name="goliath", @path="", @parent=#<Log4r::RootLogger:0x007fd1d241f108 @level=0, @outputters=[]>, @level=1, @trace=false>
  rack.multiprocess              => false
  rack.multithread               => false
  rack.run_once                  => false
  rack.version                   => [1, 0]
  status                         => {}

  stream.close                   => #<Proc:0x007fd1d2371fa8@/Users/shanebdavis/.rvm/gems/ruby-1.9.2-p290/gems/goliath-0.9.4/lib/goliath/request.rb:30>
  stream.send                    => #<Proc:0x007fd1d2371ff8@/Users/shanebdavis/.rvm/gems/ruby-1.9.2-p290/gems/goliath-0.9.4/lib/goliath/request.rb:29>
  stream.start                   => #<Proc:0x007fd1d2371f58@/Users/shanebdavis/.rvm/gems/ruby-1.9.2-p290/gems/goliath-0.9.4/lib/goliath/request.rb:31>

  start_time                     => 1328469191.131124
  time                           => 1328469191.131144
  trace                          => []
=end

module Monotable
module GoliathServer

class HelloServer < Goliath::API
  use Goliath::Rack::Params
  use Goliath::Rack::Render, ['json', 'yaml']
  def response(env)
    [200, {}, "hello"]
  end
end

class HttpServer < Goliath::API
  class << self
    attr_reader :server
    attr_reader :running

    # options
    #   :store_paths=>["path",...]
    #   :port => Fixnum - TCP port to listen on, default 8080
    #   :host => host to listen on - default "localhost"
    #   :cluster => {} => params for initializing the cluster-manager
    def start(options={},&block)
      @server = Monotable::Server.new(options)
      Log.verbose = server.verbose

      # Start up server

      Goliath.env=:production # or :development
      Log.info "Starting server on #{server.host}:#{server.port} in #{Goliath.env} mode. Watch out for stones."

      gserver = Goliath::Server.new(server.host, server.port)
      gserver.logger = Log
      gserver.api = GoliathServer::HttpServer.new
      gserver.app = Goliath::Rack::Builder.build(GoliathServer::HttpServer, gserver.api)
      gserver.plugins = []
      gserver.options = {}
      gserver.start do
        yield server if block
        ServerClient.use_synchrony = true
        @running = true

        server.post_init
        Log.info({"Cluster status" => server.cluster_manager.status}.to_yaml)
        Log.info "\nMonotable init successful."
        Log.info "Monotable now listenting on: #{server.host}:#{server.port}"
        puts "Monotable now listenting on: #{server.host}:#{server.port}" unless options[:quiet]
      end
    end
  end

  class Response
    attr_accessor :status, :content

    def initialize
      self.status = 200
      self.content_type "json"
    end

    def content_type(type)
      @content_type = type
    end

    # ignore
    def send_response
    end

    def to_rack_response
      [status, {"Content-Type"=>@content_type}, content]
    end
  end

  class PerResponse
    attr_accessor :options
    def initialize(env)
      @env = env
    end

    include ParamsAndBody

    # set by parse_params or parse_body
    attr_accessor :argument_error

    def request_options
      return @request_options if @request_options

      @request_options = {
        :response => Response.new,
        :server => GoliathServer::HttpServer.server,
        :params => parse_params(@env),
        :body => parse_body(@env),
        :method => @env.REQUEST_METHOD,
        :uri => @env.REQUEST_PATH ,
      }
      raise argument_error if argument_error # raise only the first time
      @request_options
    end

    include Monotable::HttpServer::Routes

    def to_rack_response
      response.to_rack_response
    end
  end


  def response(env)
    pr = PerResponse.new(env)
    pr.route_http_request     # all exceptions should be caught inside here and the proper response generated
    pr.to_rack_response
  rescue Exception => e
    puts "#{self.class}#response Internal Error: #{e.inspect}"
    puts "  "+e.backtrace.join("\n  ")
    [500, {}, "#{self.class}#response Exception = #{e.inspect}"]
  end
end
end
end
