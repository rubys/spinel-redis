# Ported from redis-rb v5.4.1 examples/sets.rb (verbatim flow; sinter is
# the typed two-key arity here).
# Gated copy: test/example_sets_test.rb (which sorts set replies — server
# set ordering is unspecified, and the gated copy diffs a snapshot).
require "redis"

r = Redis.new

r.del("foo-tags")
r.del("bar-tags")

puts
p "create a set of tags on foo-tags"

r.sadd("foo-tags", "one")
r.sadd("foo-tags", "two")
r.sadd("foo-tags", "three")

puts
p "create a set of tags on bar-tags"

r.sadd("bar-tags", "three")
r.sadd("bar-tags", "four")
r.sadd("bar-tags", "five")

puts
p "foo-tags"

p r.smembers("foo-tags")

puts
p "bar-tags"

p r.smembers("bar-tags")

puts
p "intersection of foo-tags and bar-tags"

p r.sinter("foo-tags", "bar-tags")
