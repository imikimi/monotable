require 'multi_json'
require 'rack/utils'

module Monotable
module GoliathServer

  module MinimalIndifferentHash
    # convert all Hashes datastructure of Arrays and Hashs to indifferent hashes
    def indifferentize(obj)
      case obj
      when Array then obj.collect {|el| indifferentize el}
      when Hash then
        ih = Hash.new {|hash,key| hash[key.to_s] if Symbol === key }
        obj.each {|k,v| ih[k]=indifferentize v}
        ih
      else
        obj
      end
    end
  end

  # differs from Goliath's Rack::Params in that it keeps the Body separate from the Params
  # Like Goliath, it sets the env['params'] value.
  # Unlike Goliath, the body is parse (if content-type is Json) and stored in env['body']
  # If the body content-type is not json, the raw body is stored in env['body']
  #
  # @example
  #  use Monotable::GoliathServer::Rack::ParamsAndBody
  module ParamsAndBody
    include MinimalIndifferentHash

    def parse_params(env)
      indifferentize ::Rack::Utils.parse_nested_query(env['QUERY_STRING'])
    rescue Exception => ae
      self.argument_error = ArgumentError.new "Invalid query_string. Error: #{ae.inspect}"
      nil
    end

    def parse_body(env)
      return unless env['rack.input']
      body = env['rack.input'].read
      env['rack.input'].rewind

      case env['CONTENT_TYPE']
      when "application/json" then
        begin
          indifferentize MultiJson.decode(body) unless body.empty?
        rescue MultiJson::DecodeError => e
          self.argument_error = ArgumentError.new "Invalid JSON in body. Error: #{e.class.to_s}. Body=#{body.inspect}"
          nil
        end
      else
        body
      end
    end
  end
end
end
