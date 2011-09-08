class Monotable::Daemon::RecordDeferrable
  include EM::Deferrable
  
  def initialize(response)
    @response = response    
  end
    
  def list
    @response.status = '200'
    @response.content_type 'text/plain'    
    @response.content = 'TODO List of records'
    @response.send_response    
  end
  
  def create(key, props)
    @response.status = '200'
    @response.content_type 'text/plain'    
    @response.content = 'TODO Create record'
    @response.send_response
  end
    
  def update(key,props)
    puts "update"
    self.callback do
      @response.status = '202'
      @response.content_type 'text/plain'    
      @response.content = 'Record written'
      @response.send_response    
    end    
    # TODO Add a errback, when the conditions are known for such a failure
    call_p, result_p = proc { Monotable::LOCAL_STORE.set(key,props) }, proc {|set_result| succeed }
    # EM.defer call_p, result_p
    result_p.call(call_p.call)
  end
  
  def read(key)
    puts "read"
    self.callback do |content|
      @response.status = '200'
      # @response.content_type 'application/octet-stream'
      @response.content_type 'text/plain'          
      @response.content = content.to_json
      @response.send_response          
    end    
    self.errback do
      @response.status = '404'
      @response.content_type 'text/plain'    
      @response.content = 'Record not found'            
      @response.send_response
    end
    call_p, result_p = proc { Monotable::LOCAL_STORE.get(key) }, proc {|get_result| get_result ? succeed(get_result) : fail }
    # EM.defer call_p, result_p
    result_p.call(call_p.call)    
  end
  
  def delete(key)
    puts "delete"
    # EM.defer proc { Monotable::LOCAL_STORE.delete(key) }, proc {|delete_result| succeed }   
    Monotable::LOCAL_STORE.delete(key)
    succeed
  end
  
end