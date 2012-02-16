# encoding: BINARY
module Monotable
  class Router < TopServerComponent
    attr_accessor :server_clients

    #options[:local_store] => LocalStore
    def initialize(server)
      super
      @server_clients=[]
    end

    #NOTE: this hard-caches the first-record-key
    # Someday the first-record-key may change, but currently are initializing the
    # store to be big enough for practically anyone's need, so adding another
    # index level won't be needed for quite a while.
    def first_record_key
      @first_record_key ||= global_index.first_record.key
    end

    # key is in the first chunk if it has the same number of "+"s as the paxos record
    def key_in_first_chunk?(internal_key)
      internal_key[0..first_record_key.length-1]==paxos_record.key
    end

    # find the servers containing the chunk that covers key
    def servers(internal_key)
      if key_in_first_chunk? internal_key
        global_index.first_record.servers
      else
        global_index.find(internal_key,self).servers
      end
    end

    def server_client(ikey)
      server_list=servers(ikey)
      server_address=server_list[rand(ss.length)]
      server.cluster_manager.server_client(server_address)
    end
  end

  # see ReadAPI
  module RoutedReadAPI
    include ReadAPI

    # see ReadAPI
    def get(key,field_names=nil)
      route(key) {|store,key| store.get(key,field_names)}
    end

    # see ReadAPI
    def get_first(options={})
      route_get_range(options,:gte) {|store,options| process_get_range_result store.get_first(options)}
    end

    # see ReadAPI
    def get_last(options={})
      route_get_range(options,:lte) {|store,options| process_get_range_result store.get_last(options)}
    end
  end

  # see WriteAPI
  module RoutedWriteAPI
    include WriteAPI

    # see WriteAPI
    def set(key,fields)
      route(key) {|store,key| store.set(key,fields)}
    end

    # see WriteAPI
    def update(key,fields)
      route(key) {|store,key| store.update(key,fields)}
    end

    # see WriteAPI
    def delete(key)
      route(key) {|store,key| store.delete(key)}
    end
  end

  class RequestRouter
    USER_SPACE_PREFIX="u/"
    include RoutedReadAPI
    include RoutedWriteAPI

    attr_accessor :router

    # options
    #   :user_keys => true, all keys going in and out are in userspace (prefixed by USER_SPACE_PREFIX)
    #   :forward => true, forward requests which cannot be fullfilled locally to the appropriate remote machine
    #       otehrwise, returns {:error=>'...'} if the request cannot be carried out locally
    def initialize(router,options={})
      @router = router
      @forward = options[:forward]
      @user_keys = options[:user_keys]
      raise ArgumentError,"router.kind_of?(Router) required (router.class==#{router.class})" unless router.kind_of?(Router)
      raise InternalError,"router.local_store not set" unless router.local_store
    end

    #routing_option should be :gte or :lte
    # yields the store to route to and the options, internalized
    def route_get_range(options,routing_option)
      Tools.normalize_range_options(options)
      route(options[routing_option]) do |store,key|
        yield store, @user_keys ? internalize_range_options(options) : options
      end
    end

    def process_get_range_result(result)
      if @user_keys
        result[:records]=result[:records].collect do |k,v|
          [externalize_key(k),v]
        end
      end
      result
    end

    # external keys, or user-space keys, are prefixed to distinguish them from other internal-use keys
    def internalize_key(external_key)
      USER_SPACE_PREFIX+external_key
    end

    def externalize_key(internal_key)
      internal_key[USER_SPACE_PREFIX.length..-1]
    end

    def internalize_range_options(range_options)
      [:gte,:gt,:lte,:lt,:with_prefix].each do |key|
        range_options[key]=internalize_key(range_options[key]) if range_options[key]
      end
      range_options
    end

    # yields store, key
    #   store => store to route to
    #   key => use this key instead of the key passed to route
    # TODO - should allow a block to be passed in which is the "what to do with the result" block
    def route(key)
      ikey = @user_keys ? internalize_key(key) : key
      work_log=[]
      #puts "routing: ikey=#{ikey[0..10].inspect}"
      ret = if router.local_store.local?(ikey)
        #puts "routing: #{ikey[0..10].inspect} -> local"
        work_log<<"processed locally"
        yield router.local_store,ikey
      else
        #puts "routing: #{ikey[0..10].inspect} -> remote"
        unless @forward
          {:error=>"key not covered by local chunks"}
        else
          sc=router.server_client(ikey)
          work_log<<"forwarding request to: #{sc}"
          yield sc,ikey
        end
      end
      ret[:work_log]=(ret[:work_log]||[])+work_log
      ret
    end
  end
end