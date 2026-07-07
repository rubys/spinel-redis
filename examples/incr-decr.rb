# Ported from redis-rb v5.4.1 examples/incr-decr.rb (verbatim flow).
# Gated copy: test/example_incr_decr_test.rb
require "redis"

r = Redis.new

puts
p "incr"
r.del("counter")

p r.incr("counter")
p r.incr("counter")
p r.incr("counter")

puts
p "decr"
p r.decr("counter")
p r.decr("counter")
p r.decr("counter")
