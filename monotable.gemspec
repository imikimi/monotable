# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "monotable/version"

Gem::Specification.new do |s|
  s.name        = "monotable"
  s.version     = Monotable::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Shane Brinkman-Davis", "Jason Strutz"]
  s.email       = ["TODO: Write your email address"]
  s.homepage    = ""
  s.summary     = %q{TODO: Write a gem summary}
  s.description = %q{TODO: Write a gem description}

  s.rubyforge_project = "monotable"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")

  s.add_dependency 'RubyInline', '~> 3.9.0'
  s.add_dependency 'rbtree', '~> 0.3.0'
#  s.add_dependency 'eventmachine' #, '~> 0.12.10'
  s.add_dependency 'eventmachine_httpserver', '>= 0.2.1'
  s.add_dependency 'em-http-request' , '>= 1.0.1'
  s.add_dependency 'json'
  s.add_dependency 'OptionParser'
  s.add_dependency 'addressable'
  s.add_dependency 'goliath'
  s.add_dependency 'trollop'
  s.add_dependency 'sys-filesystem'

  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec', '~> 2.6.0'
  s.add_development_dependency 'rest-client'
  s.add_development_dependency 'simplecov'

  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
