=begin
SBD note:

This new file_handle is probably good.

And we want the journal to open and hold-open a write and a read handle.

Currently we just have a problem with the auto-journal-compact-on-init. The journal gets created, it writes it's 0-byte file to disk
when we open it's write handle, and then it detect there is an existing journal and we try to compact it, and we start going all loopy.
=end

module MonoTable
  class FileHandle
    attr_accessor :filename
    attr_accessor :write_handle
    attr_accessor :read_handle

    def initialize(fn)
      @filename=fn
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
        block ? yield(f) : f.read(length).force_encoding("BINARY")
      end
    end

    def write(str)
      @write_handle.write(str)
    end

    def flush
      write_handle.flush if write_handle
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
