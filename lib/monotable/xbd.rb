# encoding: BINARY
#*****************************************************
# Ruby XBD Library
# (C) 2010-10-03 Shane Brinkman-Davis
#
# SRC home:
#   URL: https://svn.imikimi.com/auto_branch/2008-10-30_15-54-25_iphone_edge/xbd/xbd.rb
#   Repository Root: https://svn.imikimi.com
#   Repository UUID: d1359c2d-ec2c-0410-b0bc-eca7d5e44040
#   Revision: 11647
#*****************************************************
=begin

This code read and writes XBD files.

To get started:

Take any XBD file
  example Kimi from Imikimi.com (all Kimis are XBD containers)
    http://imikimi.com/plugin/get_kimi/o-10i

And run:
  require "xbd.rb"
  puts Xbd.load_from_file("your_xbd_filename.xbd")

Master verison of this file lives in: imikimi_plugin_source/xbd/xbd.rb

=end
require File.join(File.dirname(__FILE__),"asi")
require File.join(File.dirname(__FILE__),"xbd_dictionary")
require File.join(File.dirname(__FILE__),"xbd_tag")

#*********************************
# Xbd Module
#*********************************
module Xbd

  SBDXML_HEADER="SBDXML\1\0"

  #**********************************************************
  # XML escaping
  #**********************************************************
  # Used when converting TO xml (Xbd::Tag.to_s)
  # Note, some of these codes are actually invalid strict XML because XML has no escape codes for some values.
  #   SBD: WTF! (I'm still surprised by this, but it appears to be true.)
  XML_ESCAPE_CODES={
    "&#0;"=>0,
    "&#1;"=>1,
    "&#2;"=>2,
    "&#3;"=>3,
    "&#4;"=>4,
    "&#5;"=>5,
    "&#6;"=>6,
    "&#7;"=>7,
    "&#8;"=>8,
    "&#9;"=>9,
    "&#10;"=>10,
    "&#11;"=>11,
    "&#12;"=>12,
    "&#13;"=>13,
    "&#14;"=>14,
    "&#15;"=>15,
    "&#16;"=>16,
    "&#17;"=>17,
    "&#18;"=>18,
    "&#19;"=>19,
    "&#20;"=>20,
    "&#21;"=>21,
    "&#22;"=>22,
    "&#23;"=>23,
    "&#24;"=>24,
    "&#25;"=>25,
    "&#26;"=>26,
    "&#27;"=>27,
    "&#28;"=>28,
    "&#29;"=>29,
    "&#30;"=>30,
    "&#31;"=>31,
    "&quot;"=>34,   #"
    "&amp;"=>38,    #&
    "&apos;"=>39,   #'
    "&lt;"=>60,     #<
    "&gt;"=>62      #>
  }

  @@escape_for_xml=[]
  (0..255).each {|i| @@escape_for_xml<<i.chr}
  XML_ESCAPE_CODES.each {|k,v| @@escape_for_xml[v]=k}

  def self.xml_escape(s)
    out=""
    s.each_byte {|b| out<< @@escape_for_xml[b]}
    out
  end

  # XBD.parse accepts:
  #   a string or
  #   any object that returns a string in response to the method "read"
  #     (for example, an open file handle)
  def Xbd.parse(source)
    #treat source as a stream(file) if it isn't a string
    source=source.read.force_encoding("BINARY") if source.class!=String

    # read the header
    raise "Not a valid XBD file" unless source[0..SBDXML_HEADER.length-1]==SBDXML_HEADER
    index=SBDXML_HEADER.length

    # read each of the 3 dictionaries in order
    tagsd,index=Dictionary.parse(source,index)
    attrsd,index=Dictionary.parse(source,index)
    valuesd,index=Dictionary.parse(source,index)

    # read all tags, return the root-tag
    Tag.parse(source,index,tagsd,attrsd,valuesd)[0]
  end

  # Load XBD from filename
  def Xbd.load_from_file(filename)
    Xbd.parse(File.open(filename,"rb"))
  end

end
