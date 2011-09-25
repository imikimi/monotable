module Monotable
module Daemon
module HTTP

class RequestHandler
  attr_accessor :options

  # options:
  #   :resonse_type => :json or :html
  def initialize(response,options={})
    @options=options
    @response = response
    @response_type=options[:response_type] || :json
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
