module Monotable
module EventMachineServer

module DeferrableReadAPI
  def get(key)
    content=Monotable.local_store.get(key)
    json_response(content[:record] ? 200 : 404, content)
  end
end

module DeferrableWriteAPI

  # see WriteAPI#set
  def set(key, fields)
    content=Monotable.local_store.set(key,fields)
    json_response(200,content)
  end

  # see WriteAPI#update
  def update(key,fields)
    content=Monotable.local_store.update(key,fields)
    json_response(202,content)
  end

  # see WriteAPI#delete
  def delete(key)
    content=Monotable.local_store.delete(key)
    json_response(200,content)
  end
end

class RecordDeferrable
  include EM::Deferrable
  include DeferrableReadAPI
  include DeferrableWriteAPI

  def initialize(response)
    @response = response
  end

  def json_response(status,content)
    @response.status = status.to_s
    @response.content_type 'application/json'
    @response.content = content.to_json
    @response.send_response
  end

end

end
end
