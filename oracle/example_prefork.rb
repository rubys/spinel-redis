# Oracle twin of test/example_prefork_test.rb (real redis-rb gem,
# Process.fork instead of sp_net_fork; same discipline — connect per
# worker, after the fork).
require "redis"

port = Integer(ARGV[0] || "6379")

setup = Redis.new(host: "127.0.0.1", port: port)
setup.del("prefork:counter")
setup.del("prefork:done")
setup.close

3.times do |i|
  fork do
    w = Redis.new(host: "127.0.0.1", port: port)
    w.incr("prefork:counter")
    w.rpush("prefork:done", "worker-#{i}")
    w.close
    exit!(0)
  end
end

3.times { Process.wait }

r = Redis.new(host: "127.0.0.1", port: port)
p "counter after 3 workers"
p r.get("prefork:counter")
p "workers that reported in"
p r.lrange("prefork:done", 0, -1).sort
