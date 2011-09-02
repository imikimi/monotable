class Monotable::Daemon::RecordDeferrable
  include EM::Deferrable
  
  def initialize(response)
    @response = response    
  end
    
  def list
    puts "List"
    @response.status = '200'
    @response.content_type 'text/plain'    
    @response.content = 'TODO List of records'
    @response.send_response    
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
    EM.defer proc { LOCAL_STORE.set(key,props) }, proc {|set_result| succeed }
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
    
    EM.defer proc { LOCAL_STORE.get(key) }, proc {|get_result| get_result ? succeed(get_result) : fail }
  end
  
  
  def delete(key)
    puts "Received delete for #{key}"    
  end
  
end