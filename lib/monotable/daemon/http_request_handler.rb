module Monotable
module Daemon
module HTTP

class RequestHandler
  attr_accessor :options
  attr_reader :method, :params, :uri, :response, :response_type, :controller, :action, :post_action_path, :resource_id

  # options:
  #   :resonse_type => :json or :html
  def initialize(response,options={})
    @options = options
    @response = response
    @response_type=options[:response_type] || :json

    @method = options[:method]
    @params = options[:params]
    @uri    = options[:uri]

    ignore, @controller, @action, @post_action_path = @uri.split("/",4)
    @resource_id = @post_action_path
  end

  def handle
    handle_unknown_request
  end

  def handle_unknown_request(message=nil)
    message||=self.class.to_s
    @response.status = 406
    @response.content_type 'text/html'
    @response.content = "Unknown request: #{method} #{uri.inspect}. #{"("+message+")" if message}"
    @response.send_response
  end

  def handle_resource_missing_request(message=nil)
    message||=self.class.to_s
    @response.status = 404
    @response.content_type 'text/html'
    @response.content = "Resource could not be found: #{method} #{uri.inspect}. #{"("+message+")" if message}"
    @response.send_response
  end

  def handle_invalid_request(message=nil)
    message||=self.class.to_s
    @response.status = 406
    @response.content_type 'text/html'
    @response.content = "Invalid request: #{method} #{uri.inspect}. #{"("+message+")" if message}"
    @response.send_response
  end

  def respond(status,content)
    @response.status = status.to_s
    case @response_type
    when :json then respond_with_json(status,content)
    when :html then respond_with_html(status,content)
    end
    @response.send_response
  end

  def respond_with_json(status,content)
    @response.content_type 'application/json'
    content = Tools.force_encoding(content,"UTF-8")
    @response.content = content.to_json
  end

  def respond_with_html(status,content)
    @response.content_type 'text/html'
    @response.content = <<ENDHTML
<html>
<body>
<pre><code>
#{content.to_yaml}
</code></pre>
</body>
</html>
ENDHTML
  end
end

end
end
end
