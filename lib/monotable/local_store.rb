require File.join(File.dirname(__FILE__),"xbd")
require File.join(File.dirname(__FILE__),"version")

require 'yaml'
require 'cgi'
require 'fileutils'
require 'json'
require 'rubygems'

def monotable_require(relative_path,modules)
  modules.each do |mod|
    require File.join(File.dirname(__FILE__),relative_path.to_s,mod)
  end
end

monotable_require :local_store, %w{
  api
  string
  global
  cache
  record_cache
  index_block_cache
  constants
  tools
  file_handle
  journal
  journal_manager
  compaction_manager
  logger
  column
  columns
  record
  chunk
  index_block
  index_block_encoder
  memory_chunk
  disk_chunk_base
  disk_chunk
  path_store
  local_store
}
