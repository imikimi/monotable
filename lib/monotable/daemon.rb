require 'sinatra/async'
require 'yaml'
require 'fileutils'
require "sinatra/reloader"
require 'json'

class Monotable::Daemon < Sinatra::Base
  register Sinatra::Async
  set :views, File.dirname(__FILE__) + "/daemon/views"
  
  # SYSTEM_KEY_PREFIX = 'system_'
  # USER_KEY_PREFIX = 'user_'
  # ROOT_INDEX_NAME = SYSTEM_KEY_PREFIX + 'root_index'
  # SYSTEM_INDEX_PREFIX = SYSTEM_KEY_PREFIX + 'index_'
  
  # Index
  get '/' do
    status 200
    body erb :index
  end
  
  # Chunks List
  aget '/chunks' do
    status 200
    EM.defer(
      proc { 
        local_chunk_names
      },
      proc {|result|
        if result
          body do 
            @chunk_names = result
            if request.accept.include?('application/json')
              content_type 'application/json', :charset => 'utf-8'
              @chunk_names.to_json
            else
              erb :'chunks/index'
            end
          end
        else
          status 500
          body 'Failed'
        end
      }
    )    
  end

  # Chunk Read -- Info in html or JSON
  aget '/chunks/:name', :provides => [:html, :json] do |name|
    EM.defer(
      proc { has_chunk?(name) },
      proc {|chunk_exists|
        unless chunk_exists
          status 404
          body "Could not find chunk #{name}"
        else
          status 200          
          EM.defer(
            proc { chunk_info(name) },
            proc {|info|
              case request.accept.join
              when /application\/json/
                body info.to_json
              when /text\/html/
                @chunk_info = info
                body erb :'chunks/show'
              end              
            }
          ) # End chunk_info defer
        end
      } 
    ) # End has_chunk defer
  end
  
  # Chunk Read - Raw download
  aget '/chunks/:name', :provides => 'application/octet-stream' do |name|
    EM.defer(
      proc { has_chunk?(name) },
      proc {|chunk_exists|
        unless chunk_exists
          status 404
          body "Could not find chunk #{name}"
        else
          status 200          
          EM.defer(
            proc { read_chunk(name) },
            proc {|chunk_contents|
              body chunk_contents
            }
          ) # End read_chunk defer
        end
      } 
    ) # End has_chunk defer
  end
  
  # Chunk Read - Catch-all for wrong Accept headers
  aget '/chunks/:name' do |name|
    status 406
    body 'Not Acceptable.  Check your Accept headers; they need to be text/html, application/json, or application/octet-stream'
  end


  # Chunk Update (or create)
  aput '/chunks/:name' do |name|
    EM.defer(
      proc { 
        update_chunk(name, request.body.read) # NOTE: Might need some additional handling for streaming (100-Continue)
      },
      proc {|result|
        if result
          status 202
          body 'Accepted'
        else
          status 500
          body 'Failed'
        end
      }
    )    
  end

  # Chunk Delete
  adelete '/chunks/:name' do |name|
    EM.defer(
      proc { has_chunk?(name) },
      proc {|chunk_exists|
        unless chunk_exists
          status 404
          body "Could not find chunk #{name}"
        else
          status 202          
          EM.defer(
            proc { delete_chunk(name) },
            proc {|result|
              if result
                status 204 # Success
                body 'No Content'
              else
                status 500
                body 'Failed'
              end
            }
          ) # End delete_chunk defer
        end
      } 
    ) # End has_chunk defer
  end

  
  aget '/records/?' do
    EM.defer(
      proc {
        if params['chunk']
          before, after = local_chunk_names.partition {|c| c <= params['chunk']}
          current = before.pop
          [{:prev => before.last, :current => current, :next => after.first }, read_chunk_contents(chunk_path(current))]
        else
          current, *after = local_chunk_names
          [{:prev => nil, :current => current, :next => after.first}, read_chunk_contents(chunk_path(current))]
        end
      },
      proc {|pagination, current_chunk_contents|
        if current_chunk_contents
          @pagination, @chunk_contents = pagination, current_chunk_contents
          status 200
          body erb :'records/index'
        else
          status 500
          body 'Failed'
        end
      }
    )    
  end
  
  
  # Record Update
  aput '/records/:key' do |key|
    EM.defer(
      proc { 
        update_record(key, request.body.read) # NOTE: Might need some additional handling for streaming (100-Continue)
      },
      proc {|result|
        if result
          status 202
          body 'Accepted'
        else
          status 500
          body 'Failed'
        end
      }
    )
  end
  
  
  # Record Read
  # TODO:  Maybe accept a query string argument chunk_name, which is a hint of where the record lives?
  aget '/records/:key' do |key| 
    EM.defer(
      proc { 
        read_record(key)
      },
      proc {|record|
        if record
          status 200
          body record
        else
          status 404
          body "Record for key \"#{key}\" was not found"
        end
      }
    )    
  end
  
  # Record Delete
  adelete '/records/:key' do |key|
    EM.defer(
      proc { 
        delete_record(key)
      },
      proc {|result|
        if result
          status 204
          body 'No Content' # Success
        else
          status 500
          body 'Failed'
        end
      }
    )
  end

  protected
  # Stub methods for monotable local store methods
  # All these methods are considered synchronous, so be sure to wrap them with callbacks
  def has_chunk?(chunk_name)
    File.exist?(chunk_path(chunk_name))
  end
  
  def is_index_chunk?(chunk_name)
    chunk_name =~ /^#{SYSTEM_INDEX_PREFIX}/
  end
  
  def chunk_path(chunk_name)
    File.join(data_path, chunk_name + '.yml')
  end
  
  def data_path
    File.expand_path('./data')
  end
    
  def chunk_for_record(record_key)
    last_chunk_smaller_than_record_key = nil
    local_chunk_names.sort.each do |chunk_name|
      # puts "chunk_for_record considering #{chunk_name}"
      if chunk_name <= record_key
        # puts "\t#{chunk_name} is a candidate container for #{record_key}"
        last_chunk_smaller_than_record_key = chunk_name
      else
        # puts "\t#{chunk_name} is NOT a candidate container for #{record_key}, therefore it must be #{last_chunk_smaller_than_record_key}"        
        break
      end
    end
    return last_chunk_smaller_than_record_key
  end  
  
  def local_chunk_names
    `ls #{data_path}/*.yml 2>/dev/null`.lines.map{|f| File.basename(f.chomp,'.yml')}
  end

  def chunk_info(chunk_name)
    size_str, name = `ls -lrt #{data_path}/#{chunk_name}.yml | awk '{print $5,$9}' 2> /dev/null`.strip.split(' ',2)
    # `ls #{data_path}/*.yml 2>/dev/null`.lines.map{|f| File.basename(f.chomp,'.yml')}
    {:size => size_str.to_i, :name => chunk_name, :full_path => chunk_path(chunk_name) }
  end


  def read_chunk(chunk_name)
    File.read(chunk_path(chunk_name))
  rescue
    nil
  end

  def update_chunk(chunk_name, data)
    dest_path = chunk_path(chunk_name)
    if data.is_a?(String)
      File.open(dest_path, 'w+') {|f| f.write(data)}
    elsif data.is_a?(File)
      FileUtils.mv(data, dest_path)
    else
      nil
    end
  rescue
    nil
  end

  def delete_chunk(chunk_name)
    FileUtils.rm(chunk_path(chunk_name))
  rescue
    nil
  end


  def create_chunk_for_record(record_key)
    write_chunk_contents(chunk_path(record_key), {
      :created_at => Time.now,
      :updated_at => Time.now,
      :records => {}      
    })
    record_key
  end

  def update_record(record_key, data)
    chunk_name = chunk_for_record(record_key) || create_chunk_for_record(record_key)
    unpacked_chunk = read_chunk_contents(chunk_path(chunk_name))
    unpacked_chunk[:records][record_key] = data
    unpacked_chunk[:updated_at] = Time.now
    write_chunk_contents(chunk_path(chunk_name), unpacked_chunk)
  rescue => e
    # puts e, e.backtrace
    nil
  end

  def read_record(record_key)
    read_chunk_contents(chunk_path(chunk_for_record(record_key)))[:records][record_key]
  rescue => e
    # puts e, e.backtrace
    nil
  end

  def delete_record(record_key)
    chunk_name = chunk_for_record(record_key) || create_chunk_for_record(record_key)
    unpacked_chunk = read_chunk_contents(chunk_path(chunk_name))
    unpacked_chunk[:records].delete(record_key)
    unpacked_chunk[:updated_at] = Time.now
    write_chunk_contents(chunk_path(chunk_name), unpacked_chunk)    
  rescue => e
    # puts e, e.backtrace    
    nil
  end

  private
  def read_chunk_contents(path)
    # For now, chunks are YAML files, with the following structure
    # { :created_at => Timestamp,
    #   :updated_at => Timestamp,
    #   :records => Hash of key => value
    # }    
    YAML.load_file( path )
  end
  
  def write_chunk_contents(file_path, unpacked_chunk)
    File.open(file_path, 'w+') {|f| YAML.dump(unpacked_chunk,f) }
  end

    

end