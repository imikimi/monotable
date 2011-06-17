# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "monotable/version"

Gem::Specification.new do |s|
  s.name        = "monotable"
  s.version     = Monotable::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["TODO: Write your name"]
  s.email       = ["TODO: Write your email address"]
  s.homepage    = ""
  s.summary     = %q{TODO: Write a gem summary}
  s.description = %q{TODO: Write a gem description}

  s.rubyforge_project = "monotable"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  
  s.add_dependency 'rbtree', '~> 0.3.0'
  s.add_dependency 'sinatra', '~> 1.2.2'
  s.add_dependency 'async_sinatra', '~> 0.5.0'
  s.add_dependency 'thin', '~> 1.2.11'
  
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
