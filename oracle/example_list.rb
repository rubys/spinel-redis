# Oracle twin of test/example_list_test.rb (real redis-rb gem).
require "redis"

r = Redis.new(host: "127.0.0.1", port: Integer(ARGV[0] || "6379"))

r.del("logs")

puts

p "pushing log messages into a LIST"
r.rpush("logs", "some log message")
r.rpush("logs", "another log message")
r.rpush("logs", "yet another log message")
r.rpush("logs", "also another log message")

puts
p "contents of logs LIST"

p r.lrange("logs", 0, -1)

puts
p "Trim logs LIST to last 2 elements(easy circular buffer)"

r.ltrim("logs", -2, -1)

p r.lrange("logs", 0, -1)
