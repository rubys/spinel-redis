# Oracle twin of test/example_sets_test.rb (real redis-rb gem; sorted,
# matching the gated copy).
require "redis"

r = Redis.new(host: "127.0.0.1", port: Integer(ARGV[0] || "6379"))

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

p r.smembers("foo-tags").sort

puts
p "bar-tags"

p r.smembers("bar-tags").sort

puts
p "intersection of foo-tags and bar-tags"

p r.sinter("foo-tags", "bar-tags").sort
