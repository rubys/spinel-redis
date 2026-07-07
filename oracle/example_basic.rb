# Oracle twin of test/example_basic_test.rb — same flow through the REAL
# redis-rb gem. Run via oracle/run.sh (never under spin: no -I, so
# `require "redis"` resolves to the gem, not this package).
require "redis"

r = Redis.new(host: "127.0.0.1", port: Integer(ARGV[0] || "6379"))

r.del("foo")

puts

p 'set foo to "bar"'
r.set("foo", "bar")

puts

p "value of foo"
p r.get("foo")
