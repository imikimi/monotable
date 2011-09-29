![Monotable](https://github.com/imikimi/monotable/raw/gh-pages/images/monotable.png)

## What is it?

Monotable aims to provide a reliable distributed key-value data store, intended primarily for storing large numbers of small files.  Monotable is implemented in a combination of Ruby and C.

## Current status

*Monotable is in "proof-of-concept" stage, in which we are translating our design on paper into a working prototype.*

## FAQ

### Why another data store?

Given that there are plenty of good data stores, why undertake writing another one?  Our primary goals are:

* Store approximately a petabyte of data
* Store billions of records
* Store binary records (files) 10kB-10mB
* Flexible administration, indexing, replication, and growth

The ecosystem of existing data stores offered no clear winners.

### Why Ruby?

Systems languages reign supreme for implementing a data store, so Ruby is an uncommon language choice.  Here are the pros and cons for choosing ruby:

* Pros
  * Ruby is a productive language.  Ease of use translates to fewer bugs and quicker development.
  * A data store will ideally be disk or network IO bound, which narrows the performance disparity between systems languages and higher-level languages such as Ruby.
  * Ruby has excellent network daemon support in the form of [EventMachine](http://rubyeventmachine.com/).
* Cons
  * The official ruby interpreter (MRI) has some notable performance limitations, including a non-generational garbage collector and a global interpreter lock (GIL), which prevents truly simultaneous execution.  *How to overcome it*: There are alternative ruby interpreters which are not subject to these limitations, such as [JRuby](http://jruby.org/) and [Rubinius](http://rubini.us/).
  * The high level nature of ruby means it will be significantly slower for lower level operations.  *How to overcome it*: For insurmountable CPU performance bottlenecks in Ruby, break out of ruby down to C, using [RubyInline](http://www.zenspider.com/ZSS/Products/RubyInline/) or external libraries with [FFI](http://wiki.github.com/ffi/ffi).

### Why is it called "Monotable"?

It is a "table" in that it has a primary key sort, like many other databases. It is "mono" in that it only has *one* primary key sort.  Multiple tables can be logically created by namespacing keys.  To be clear, though it is a single table, it can span a large number of machines.

### What do you mean by "flexible"?

From the client's perspective, a data store should appear to be "one giant disk".  To best satisfy this requirement, a data store will need the following:

* Adding a node to the cluster should just require starting a single daemon and pointing it to an existing cluster.
* Removing a node should be as simple as turning it off.
* Removing a node should not leave the cluster in a degenerate state.
* A node should have a single daemon.
* A cluster should have no single point of failure.
* You can access any data in the cluster by accessing any node in the cluster.
* No cluster restarts for configuration changes.

## Contributing

Development is in the early stages.  Please message us via Github if you'd like to help out.
