begin
  require 'rspec'
  require 'rspec/core/rake_task'

  desc "Run all examples with RCov"
  RSpec::Core::RakeTask.new('spec:rcov') do |t|
    t.rcov = true
  end

  RSpec::Core::RakeTask.new('spec') do |t|
    t.verbose = true
  end

  desc "Run all local_store RSpec code examples"
  RSpec::Core::RakeTask.new('spec:local_store') do |t|
    t.verbose = true
    t.pattern = "spec/local_store_spec/*spec.rb"
  end

  desc "Run all server RSpec code examples"
  RSpec::Core::RakeTask.new('spec:server') do |t|
    t.verbose = true
    t.pattern = "spec/server_spec/*spec.rb"
  end

  task :default => :spec
rescue LoadError
  puts "rspec, or one of its dependencies, is not available. Install it with: sudo gem install rspec"
end
