module Monotable
module Daemon
module HTTP

class InternalRequestHandler < RequestHandler
#  include EM::Deferrable

  def chunks()
    content={:chunks=>server.local_store.chunks.keys}
    respond(200, content)
  end
end

end
end
end
