# End-to-end smoke against a real redis-server, spinel-only: the committed
# .expected snapshot keeps spin test from ever running this under CRuby
# (the ffi graph can't load there). The test owns its server: private
# port, no persistence, shut down at the end.
#
# Requires redis-server + redis-cli on PATH.
require "redis"

module SmokeShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

SMOKE_PORT = 16391

def smoke_connect(port)
  attempts = 0
  while true
    begin
      # assign-then-return, not `return Redis.new(...)`: matz/spinel#1775
      # pops the rescue frame before evaluating a return expression
      c = Redis.new("127.0.0.1", port)
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "smoke: redis-server did not come up on port " + port.to_s
      end
      SmokeShell.sp_net_shell_capture("sleep 0.1", 16)
    end
  end
end

SmokeShell.sp_net_shell_capture("redis-server --port 16391 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-smoke.pid --logfile /tmp/spinel-redis-smoke.log", 256)

r = smoke_connect(SMOKE_PORT)
r.flushdb

puts "ping         " + r.ping
puts "echo_utf8    " + r.echo("héllo wörld")

# strings
puts "set          " + r.set("smoke:k", "value-1")
g = r.get("smoke:k")
puts "get          " + g.to_s
m = r.get("smoke:missing")
puts "get_missing  " + m.nil?.to_s
puts "append       " + r.append("smoke:k", "!").to_s
puts "strlen       " + r.strlen("smoke:k").to_s
puts "exists_yes   " + r.exists?("smoke:k").to_s
puts "exists_no    " + r.exists?("smoke:missing").to_s

# counters
puts "incr         " + r.incr("smoke:n").to_s
puts "incrby       " + r.incrby("smoke:n", 41).to_s
puts "decr         " + r.decr("smoke:n").to_s

# expiry
puts "expire       " + r.expire("smoke:k", 1000).to_s
t = r.ttl("smoke:k")
puts "ttl_bounded  " + (t > 0 && t <= 1000).to_s

# lists
r.rpush("smoke:list", "a")
r.rpush("smoke:list", "b")
r.lpush("smoke:list", "z")
puts "llen         " + r.llen("smoke:list").to_s
l = r.lrange("smoke:list", 0, -1)
puts "lrange       " + l[0] + l[1] + l[2]
puts "lpop         " + r.lpop("smoke:list").to_s
puts "rpop         " + r.rpop("smoke:list").to_s

# sets
puts "sadd_1       " + r.sadd("smoke:set", "m1").to_s
puts "sadd_dup     " + r.sadd("smoke:set", "m1").to_s
r.sadd("smoke:set", "m2")
puts "scard        " + r.scard("smoke:set").to_s
puts "sismember_y  " + r.sismember("smoke:set", "m1").to_s
puts "sismember_n  " + r.sismember("smoke:set", "nope").to_s
puts "srem         " + r.srem("smoke:set", "m1").to_s

# hashes
puts "hset         " + r.hset("smoke:h", "f1", "v1").to_s
r.hset("smoke:h", "f2", "v2")
h = r.hgetall("smoke:h")
puts "hgetall      " + h.length.to_s + ":" + h["f1"] + ":" + h["f2"]
hg = r.hget("smoke:h", "f1")
puts "hget         " + hg.to_s
puts "hdel         " + r.hdel("smoke:h", "f1").to_s

# binary round trip THROUGH the server: NUL + CRLF in the value must
# survive both the send path (write_bytes w/ explicit length) and the
# recv path (:binstr). This is the ffi write-side NUL probe.
bin = [97, 0, 98, 13, 10, 99].pack("C*")
r.set("smoke:bin", bin)
v = r.get("smoke:bin")
bin_ok = false
if !v.nil?
  bin_ok = v.bytesize == 6
  bin_ok = bin_ok && v.getbyte(1) == 0
  bin_ok = bin_ok && v == bin
end
puts "binary_rt    " + bin_ok.to_s

# error surface: wrong type
r.set("smoke:str", "x")
raised = false
begin
  r.incr("smoke:str")
rescue => e
  raised = e.message.include?("not an integer")
end
puts "err_wrongty  " + raised.to_s

# publish with no subscribers
puts "publish_0    " + r.publish("smoke:chan", "hello").to_s

# cleanup
puts "del          " + r.del("smoke:k").to_s
r.close
SmokeShell.sp_net_shell_capture("redis-cli -p 16391 shutdown nosave 2>/dev/null", 64)
puts "done"
