# encoding: BINARY
require "fileutils"
require 'digest/md5'
=begin
This implementation of DiskChunk loads all the leaf IndexBlocks and fully populates @records.
  Fields are loaded on demand.
  Writes are journaled.
=end

module Monotable

  class DiskChunk < DiskChunkBase
  end
end
