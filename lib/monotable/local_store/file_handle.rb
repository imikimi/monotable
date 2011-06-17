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
    # If there is no block, the file is opened and left open
    def open_read(&block)
      if read_handle
        yield read_handle
        return
      end
      close
      @read_handle=File.open(filename,"rb")
      if block
        begin
          yield read_handle if block
        ensure
          close
        end
      end
    end

    def open_write(&block)
      if @write_handle
        return yield @write_handle if block
        return
      end
      close
      @write_handle=File.open(filename,"wb")
      if block
        begin
          yield @write_handle
        ensure
          close
        end
      end
    end

    def open_append(&block)
      if @write_handle
        return yield @write_handle if block
        return
      end
      close
      @write_handle=File.open(filename,"a+b")
      if block
        begin
          yield @write_handle
        ensure
          close
        end
      end
    end

    def close
      @write_handle.close if @write_handle
      @read_handle.close if @read_handle
      @write_handle=@read_handle=nil
    end

    def to_s; filename end

    def exists?() File.exists?(filename) end

    # delets the file
    def delete
      close
      FileUtils.rm [filename]
    end

    def read(offset=nil,length=nil,&block)
      open(:read) do |f|
        f.seek(offset) if offset
        block ? yield(f) : f.read(length).force_encoding("BINARY")
      end
    end

    def flush
      write_handle.flush if write_handle
    end

    def length
      File.exists?(filename) ? File.stat(filename).size : 0
    end
    alias :size :length

    # returns offset data was written to
    def append(data=nil,&block)
      open_append {|f|block ? yield(f) : f.write(data)}
    end
  end
end
