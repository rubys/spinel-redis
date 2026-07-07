# Oracle twin of test/live_smoke_test.rb (real redis-rb gem): every line
# of the committed snapshot re-derived from redis-rb's return values —
# nil for missing keys, booleans from exists?/expire/sismember, Integer
# from sadd, Hash from hgetall, binary-safe values, CommandError text.
require "redis"

r = Redis.new(host: "127.0.0.1", port: Integer(ARGV[0] || "6379"))
r.flushdb

puts "ping         " + r.ping
puts "echo_utf8    " + r.echo("héllo wörld")

puts "set          " + r.set("smoke:k", "value-1")
g = r.get("smoke:k")
puts "get          " + g.to_s
m = r.get("smoke:missing")
puts "get_missing  " + m.nil?.to_s
puts "append       " + r.append("smoke:k", "!").to_s
puts "strlen       " + r.strlen("smoke:k").to_s
puts "exists_yes   " + r.exists?("smoke:k").to_s
puts "exists_no    " + r.exists?("smoke:missing").to_s

puts "incr         " + r.incr("smoke:n").to_s
puts "incrby       " + r.incrby("smoke:n", 41).to_s
puts "decr         " + r.decr("smoke:n").to_s

puts "expire       " + r.expire("smoke:k", 1000).to_s
t = r.ttl("smoke:k")
puts "ttl_bounded  " + (t > 0 && t <= 1000).to_s

r.rpush("smoke:list", "a")
r.rpush("smoke:list", "b")
r.lpush("smoke:list", "z")
puts "llen         " + r.llen("smoke:list").to_s
l = r.lrange("smoke:list", 0, -1)
puts "lrange       " + l[0] + l[1] + l[2]
puts "lpop         " + r.lpop("smoke:list").to_s
puts "rpop         " + r.rpop("smoke:list").to_s

puts "sadd_1       " + r.sadd("smoke:set", "m1").to_s
puts "sadd_dup     " + r.sadd("smoke:set", "m1").to_s
r.sadd("smoke:set", "m2")
puts "scard        " + r.scard("smoke:set").to_s
puts "sismember_y  " + r.sismember("smoke:set", "m1").to_s
puts "sismember_n  " + r.sismember("smoke:set", "nope").to_s
puts "srem         " + r.srem("smoke:set", "m1").to_s

puts "hset         " + r.hset("smoke:h", "f1", "v1").to_s
r.hset("smoke:h", "f2", "v2")
h = r.hgetall("smoke:h")
puts "hgetall      " + h.length.to_s + ":" + h["f1"] + ":" + h["f2"]
hg = r.hget("smoke:h", "f1")
puts "hget         " + hg.to_s
puts "hdel         " + r.hdel("smoke:h", "f1").to_s

bin = [97, 0, 98, 13, 10, 99].pack("C*")
r.set("smoke:bin", bin)
v = r.get("smoke:bin")
bin_ok = false
if !v.nil?
  bin_ok = v.bytesize == 6
  bin_ok = bin_ok && v.getbyte(1) == 0
end
puts "binary_rt    " + bin_ok.to_s

r.set("smoke:str", "x")
raised = false
begin
  r.incr("smoke:str")
rescue => e
  raised = e.message.include?("not an integer")
end
puts "err_wrongty  " + raised.to_s

puts "publish_0    " + r.publish("smoke:chan", "hello").to_s

puts "del          " + r.del("smoke:k").to_s
r.close
puts "done"
