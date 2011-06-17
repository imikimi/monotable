#!/usr/bin/ruby
system("rsync -vrtl . --exclude '.git/' --delete shanebdavis@rubyforge.org:/var/www/gforge-projects/babel-bridge/")
