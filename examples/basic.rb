# Ported from redis-rb v5.4.1 examples/basic.rb — with one correction the
# oracle harness forced: upstream's `r['foo'] = 'bar'` sugar was removed
# in redis-rb 5.0, so the upstream example as written now falls through
# method_missing and sends a literal `[]=` command to the server
# (ERR unknown command). The gem's real 5.x surface is set/get.
# Gated copy: test/example_basic_test.rb
require "redis"

r = Redis.new

r.del("foo")

puts

p 'set foo to "bar"'
r.set("foo", "bar")

puts

p "value of foo"
p r.get("foo")
