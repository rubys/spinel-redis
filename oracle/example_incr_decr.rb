# Oracle twin of test/example_incr_decr_test.rb (real redis-rb gem).
require "redis"

r = Redis.new(host: "127.0.0.1", port: Integer(ARGV[0] || "6379"))

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
