module Monotable
module Daemon
module HTTP

class InternalRequestHandler < RequestHandler
#  include EM::Deferrable

  def get_root_record(key,fields={})
    content=Monotable::Daemon.local_store.get(key)
    respond(content[:record] ? 200 : 404, content)
  end
end

end
end
end
