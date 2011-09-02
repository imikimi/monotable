class Monotable::Daemon::RecordDeferrable
  include EM::Deferrable
  
  def initialize(response)
    @response = response    
  end
    
  def list
    puts "List"
  end
  
  def create(key, props)
    puts "Received create for #{key}"    
  end
    
  def update(key,props)
    puts "Received update for #{key}"
    self.callback do
      @response.status = '202'
      @response.content_type 'text/plain'    
      @response.content = 'Record written'
      @response.send_response    
    end    
    # TODO Add a errback, when the conditions are known for such a failure
    # TODO Put this in a defer
    LOCAL_STORE.set(key,props)
    succeed
  end
  
  def read(key)
    puts "Received read for #{key}"    
    self.callback do |content|
      @response.status = '200'
      # @response.content_type 'application/octet-stream'
      puts "read of key #{key}"
      @response.content_type 'text/plain'          
      @response.content = content.inspect
      @response.send_response          
    end    
    self.errback do
      @response.status = '404'
      @response.content_type 'text/plain'    
      @response.content = 'Record not found'            
      @response.send_response
    end
    
    # TODO Do this in a defer
    content = LOCAL_STORE.get(key) 
    content ? succeed(content) : fail
  end
  
  
  def delete(key)
    puts "Received delete for #{key}"    
  end
  
end