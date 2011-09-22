# encoding: BINARY
module Monotable
  # see ReadAPI
  module ServerClientReadAPI
    include ReadAPI

    # see ReadAPI
    def get(key,field_names=nil)
      raise InternalError,"not_implemented_yet"
    end

    # see ReadAPI
    def get_first(options={})
      raise InternalError,"not_implemented_yet"
    end

    # see ReadAPI
    def get_last(options={})
      raise InternalError,"not_implemented_yet"
    end
  end

  # see WriteAPI
  module ServerClientWriteAPI
    include WriteAPI

    # see WriteAPI
    def set(key,fields)
      raise InternalError,"not_implemented_yet"
    end

    # see WriteAPI
    def update(key,fields)
      raise InternalError,"not_implemented_yet"
    end

    # see WriteAPI
    def delete(key)
      raise InternalError,"not_implemented_yet"
    end
  end

  class ServerClient
    include ServerClientReadAPI
    include ServerClientWriteAPI
    attr_accessor :server

    def initialize(server)
      @server=server
    end

    def to_s
      @server
    end
  end
end
