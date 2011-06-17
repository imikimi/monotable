$: << './lib'
$: << './lib/monotable'

require 'monotable'
require 'daemon.rb'
use Rack::Static, :urls => ['/stylesheets', '/images', 'javascripts'], :root => 'public'

run Monotable::Daemon