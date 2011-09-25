module Monotable
module Daemon
module HTTP

class RecordRequestHandler < RequestHandler
#  include EM::Deferrable

  # see Monotable::ReadAPI
  module ReadAPI
    # see Monotable::ReadAPI#get
    def get(key,fields={})
      content=@store.get(key)
      respond(content[:record] ? 200 : 404, content)
    end
  end

  # see Monotable::WriteAPI
  module WriteAPI

    # see Monotable::WriteAPI#set
    def set(key,fields)
      content=@store.set(key,fields)
      respond(200,content)
    end

    # see Monotable::WriteAPI#update
    def update(key,fields)
      content=@store.update(key,fields)
      respond(202,content)
    end

    # see Monotable::WriteAPI#delete
    def delete(key)
      content=@store.delete(key)
      respond(200,content)
    end
  end

  include ReadAPI
  include WriteAPI

  def initialize(response,options={})
    super
    (@store = options[:store])
    raise(ArgumentError,"options[:store] required") unless @store
  end
end

end
end
end
