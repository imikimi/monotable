require 'tmpdir'
require 'fileutils'
require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

module Monotable
describe FileHandle do

  def in_tmp_dir
    tempdir = Dir.mktmpdir
    yield tempdir
    FileUtils.rm_rf tempdir
  end

  it "should me able to read the file into memory" do
    fh = FileHandle.new __FILE__
    data = fh.read
    data.length.should == File.stat(fh.filename).size
  end

  it "should me able to get the filename" do
    fh = FileHandle.new __FILE__
    fh.filename.should== __FILE__
    fh.to_s.should== __FILE__
  end

  it "should me able to detect if files exist" do
    fh = FileHandle.new __FILE__
    (!!fh.exists?).should == true
  end

  it "should me able to write to a file in a block" do
    in_tmp_dir do |tempdir|
      fh = FileHandle.new(File.join(tempdir,"temp"))
      data = "hi"*100
      fh.open_write do |f|
        f.write( data )
      end
      data.length.should == File.stat(fh.filename).size
    end
  end

  it "should me able to write to a file" do
    in_tmp_dir do |tempdir|
      fh = FileHandle.new(File.join(tempdir,"temp"))
      data = "hi"*100
      fh.write( data )
      data.length.should == File.stat(fh.filename).size
      FileUtils.rm_rf tempdir
    end
  end

  it "should me able to append to a file" do
    in_tmp_dir do |tempdir|
      fh = FileHandle.new(File.join(tempdir,"temp"))
      data = "hi"*100
      fh.write data
      fh.append data
      (data.length*2).should == File.stat(fh.filename).size
      FileUtils.rm_rf tempdir
    end
  end

  it "should support deleting files" do
    in_tmp_dir do |tempdir|
      fh = FileHandle.new(File.join(tempdir,"temp"))
      data = "hi"*100
      fh.write( data )
      (!!fh.exists?).should == true
      fh.delete
      (!!fh.exists?).should == false
    end
  end
end
end
