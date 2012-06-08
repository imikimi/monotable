=begin
SBD note:

This new file_handle is probably good.

And we want the journal to open and hold-open a write and a read handle.

Currently we just have a problem with the auto-journal-compact-on-init. The journal gets created, it writes it's 0-byte file to disk
when we open it's write handle, and then it detect there is an existing journal and we try to compact it, and we start going all loopy.
=end

module Monotable
  class FileHandle
    attr_accessor :filename
    attr_accessor :write_handle
    attr_accessor :read_handle
    attr_accessor :write_mutex

    def initialize(fn)
      @filename=fn
      @write_mutex=Mutex.new
    end

    def inspect
      "FileHandle<#{filename.inspect}>"
    end

    def open(mode=:read,&block)
      case mode
      when :read then open_read(&block)
      when :write then open_write(&block)
      when :append then open_append(&block)
      end
    end

    # if a block, the open file handle is yielded just like File.open
    #     If the file is already open, the open handle is used and NOT closed after.
    #     If the file is not open, it is opened and then closed
    # If there is no block, the file is opened and left open, or NOOP if already open
    def open_read(hold_open=false,&block)
      hold_open||=@read_handle # if already open, keep open
      @read_handle=File.open(filename,"rb") unless @read_handle
      yield @read_handle if block
    ensure
      close_read unless hold_open
    end

    # if a block, the open file handle is yielded just like File.open
    #     If the file is already open, the open handle is used and NOT closed after.
    #     If the file is not open, it is opened and then closed
    # If there is no block, the file is opened and left open, or NOOP if already open
    def open_write(hold_open=false,&block)
      hold_open||=@write_handle # if already open, keep open
      @write_handle=File.open(filename,"wb") unless @write_handle
      yield @write_handle if block
    ensure
      close_write unless hold_open
    end

    # same as FileHandl#open_append except the block gets exclusive access to the append stream.
    #
    # NOTE: this only works if all appendrs use open_exclusive_append.
    def open_exclusive_append(hold_open=false,&block)
      hold_open||=@append_handle # if already open, keep open
      @append_handle=File.open(filename,"wb") unless @append_handle
      @write_mutex.synchronize do
        yield @append_handle if block
      end
    ensure
      close_append unless hold_open
    end

    def open_append(hold_open=false,&block)
      hold_open||=@write_handle # if already open, keep open
      @write_handle=File.open(filename,"a+b") unless @write_handle
      yield @write_handle if block
    ensure
      close_write unless hold_open
    end

    def close_read
      @read_handle.close if @read_handle
      @read_handle=nil
    end

    def close_write
      @write_handle.close if @write_handle
      @write_handle=nil
    end

    def close
      close_read
      close_write
    end

    def to_s; filename end

    def exists?() File.exists?(filename) end

    # delets the file
    def delete
      close
      FileUtils.rm [filename]
    end

    def read(offset=nil,length=nil,hold_open=false,&block)
      open_read(hold_open) do |f|
        f.seek(offset) if offset
        length ||= size - (offset||0)
        block ? yield(f) : f.read(length).force_encoding("BINARY")
      end
    end

    def write(str)
      if @write_handle
        @write_handle.write(str)
      else
        open_write {|f| f.write(str)}
      end
    end

    def flush
      write_handle.fsync if write_handle
    end

    def length
      File.exists?(filename) ? File.stat(filename).size : 0
    end
    alias :size :length

    # returns offset data was written to
    def append(data=nil,hold_open=false,&block)
      open_append(hold_open) {|f|block ? yield(f) : f.write(data)}
    end
  end
end
