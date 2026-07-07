# Ported from redis-rb v5.4.1 examples/basic.rb (verbatim flow).
# Gated copy: test/example_basic_test.rb
require "redis"

r = Redis.new

r.del("foo")

puts

p 'set foo to "bar"'
r["foo"] = "bar"

puts

p "value of foo"
p r["foo"]
