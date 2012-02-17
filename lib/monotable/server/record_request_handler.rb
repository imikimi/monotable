module Monotable
module HttpServer

class RecordRequestHandler < RequestHandler

  attr_accessor :internal
  def body; @body||=options[:body]; end

=begin
Valid Patterns:
  /records/[key]
  /first_records/gt/[key]
  /first_records/gte/[key]
  /first_records/with_prefix/[key]
  /last_records/lt/[key]
  /last_records/lte/[key]
  /last_records/with_prefix/[key]
=end
  RECORDS_REQUEST_PATTERN = /^\/records(?:\/?(.*))$/
  FIRST_RECORDS_REQUEST_PATTERN = /^\/first_records\/(gt|gte|with_prefix)(\/(.+)?)?$/
  LAST_RECORDS_REQUEST_PATTERN = /^\/last_records\/(gt|gte|lt|lte|with_prefix)(\/(.+)?)?$/

  def parse_uri
    super
    @store = if @uri[/^\/internal\//]
      Monotable::RequestRouter.new(server.router)
    else
      Monotable::RequestRouter.new(server.router,:user_keys => true,:forward => true)
    end
  end

  def handle
    #puts "request: #{method}:#{uri}"

    rest = @uri.split(/^\/internal/)[-1]
    case rest
    when RECORDS_REQUEST_PATTERN then
      handle_record_request($1)
    when FIRST_RECORDS_REQUEST_PATTERN then
      params[$1] = $3
      handle_first_last_request(params) {|o| get_first(o)}
    when LAST_RECORDS_REQUEST_PATTERN then
      params[$1] = $3
      handle_first_last_request(params) {|o| get_last(o)}
    else
      return handle_invalid_request
    end
  end

  def handle_record_request(key)
    case method
    when 'GET'    then get(key)
    when 'POST'   then set(key,body)
    when 'PUT'    then update(key,body)
    when 'DELETE' then delete(key)
    else handle_unknown_request
    end
  end

  VALID_FIRST_LAST_PARAMS=%w{ lt lte gt gte with_prefix limit fields }

  # the params with the keys symbolized if all params are in the valid_params list,
  # else this sets up an invalid_request response
  def validate_params(valid_params,p=nil)
    p||=params
    count=0
    valid_params.each do |kstr|
      count+=1 if p.has_key? kstr
    end
    if count!=p.length
      handle_invalid_request "Query-string parameters must be one of: #{valid_params.inspect}"
      false
    else
      Hash[p.collect {|k,v| [k.to_sym,v]}]
    end
  end

  def handle_first_last_request(options)
    return unless options=validate_params(VALID_FIRST_LAST_PARAMS,options)
    options[:limit]=options[:limit].to_i if options[:limit]
    return handle_unknown_request unless method=='GET'
    yield options
  end

  # see Monotable::ReadAPI
  module ReadAPI
    # see Monotable::ReadAPI#get
    def get(key,fields={})
      content=@store.get(key)
      respond(content[:record] ? 200 : 404, content)
    end

    def deobjectify_records(result)
      puts "result = #{result.inspect}"
      result[:records] = result[:records].collect do |rec|
        [rec.key,rec.fields]
      end
      result
    end

    # see Monotable::ReadAPI#get_first
    def get_first(options={})
      #puts "get_first options=#{options.inspect}"
      content=deobjectify_records @store.get_first(options)
      respond(200,content)
    end

    # see Monotable::ReadAPI#get_last
    def get_last(options={})
      #puts "get_last options=#{options.inspect}"
      content=deobjectify_records @store.get_last(options)
      respond(200,content)
    end
  end

  # see Monotable::WriteAPI
  module WriteAPI

    # see Monotable::WriteAPI#set
    def set(key,fields)
      fields=Tools.force_encoding(fields,"ASCII-8BIT")
      content=@store.set(key,fields)
      respond(200,content)
    end

    # see Monotable::WriteAPI#update
    def update(key,fields)
      fields=Tools.force_encoding(fields,"ASCII-8BIT")
      content=@store.update(key,fields)
      respond(200,content)
    end

    # see Monotable::WriteAPI#delete
    def delete(key)
      content=@store.delete(key)
      respond(200,content)
    end
  end

  include ReadAPI
  include WriteAPI
end

end
end
