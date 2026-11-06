# Live flow for multi (MULTI ... EXEC) and the commands Sidekiq's
# heartbeat and dead set need: the dead-set write and the heartbeat's
# transactions as Sidekiq 8.1.7 sends them, an error inside EXEC and one
# at queue time, then SET NX/EX, UNLINK, multi-field HSET, variadic
# LPUSH/SADD and SSCAN on their own. Spinel-only (committed .expected);
# oracle/multi_live.rb replays it through the real redis-rb gem.
#
# Requires redis-server + redis-cli on PATH.
require "redis"

module MultiShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

def multi_connect
  attempts = 0
  while true
    begin
      c = Redis.new(url: "redis://127.0.0.1:16434/3")   # assign-then-return: matz/spinel#1775
      c.ping
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "multi test: redis-server did not come up on 16434"
      end
      MultiShell.sp_net_shell_capture("sleep 0.1", 16)
    end
  end
end

MultiShell.sp_net_shell_capture("redis-server --port 16434 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-multi.pid --logfile /tmp/spinel-redis-multi.log", 256)
r = multi_connect
r.flushdb

# Sidekiq's dead-set write (Sidekiq 8.1.7's DeadSet#kill): the job,
# then both trims, in one transaction
now = 1791599858.5
res = r.multi do |tx|
  tx.zadd("dead", now, "job-1")
  tx.zremrangebyscore("dead", "-inf", now - 15552000)
  tx.zremrangebyrank("dead", 0, -10000)
end
puts "dead_1       " + res.length.to_s + " " + res[0].to_s + " " + res[1].to_s + " " + res[2].to_s
r.zadd("dead", 1.0, "ancient")
res = r.multi do |tx|
  tx.zadd("dead", now + 1, "job-2")
  tx.zremrangebyscore("dead", "-inf", now - 15552000)
  tx.zremrangebyrank("dead", 0, -3)
end
puts "dead_2       " + res[0].to_s + " " + res[1].to_s + " " + res[2].to_s
puts "dead_set     " + r.zrange("dead", 0, -1).join(",")

# the heartbeat's main transaction (Sidekiq 8.1.7's Launcher#heartbeat)
key = "host:1:0123456789ab"
res = r.multi do |tx|
  tx.sadd("processes", [key])
  tx.exists(key)
  tx.hset(key, {"info" => "{}", "busy" => 0, "beat" => now, "rtt_us" => 120, "quiet" => "false", "rss" => 51200})
  tx.expire(key, 60)
  tx.rpop(key + "-signals")
end
puts "beat_1       " + res[0].to_s + " " + res[1].to_s + " " + res[2].to_s + " " + res[3].to_s + " " + res[4].nil?.to_s
r.lpush(key + "-signals", "TSTP")
res = r.multi do |tx|
  tx.sadd("processes", [key])
  tx.exists(key)
  tx.hset(key, {"busy" => 1, "beat" => now + 10})
  tx.expire(key, 60)
  tx.rpop(key + "-signals")
end
puts "beat_2       " + res[0].to_s + " " + res[1].to_s + " " + res[2].to_s + " " + res[3].to_s + " " + res[4].to_s
puts "beat_fields  " + r.hget(key, "busy").to_s + " " + r.hget(key, "rss").to_s + " " + r.ttl(key).to_s

# the work hash: UNLINK, then one HSET per busy thread, then EXPIRE
work = key + ":work"
r.hset(work, "stale", "x")
res = r.multi do |tx|
  tx.unlink(work)
  tx.hset(work, "t1", "{\"queue\":\"default\"}")
  tx.hset(work, "t2", "{\"queue\":\"mailers\"}")
  tx.expire(work, 60)
end
puts "work         " + res[0].to_s + " " + res[1].to_s + " " + res[2].to_s + " " + res[3].to_s
puts "work_keys    " + r.hgetall(work).keys.sort.join(",")

# SET NX/EX and variadic LPUSH inside a transaction
res = r.multi do |tx|
  tx.set("lock", "a", nx: true, ex: 30)
  tx.set("lock", "b", nx: true)
  tx.set("plain", "c")
  tx.lpush("pushed", ["x", "y", "z"])
end
puts "tx_mixed     " + res[0].to_s + " " + res[1].to_s + " " + res[2].to_s + " " + res[3].to_s
puts "lock         " + r.get("lock").to_s + " " + (r.ttl("lock") > 0).to_s
puts "pushed       " + r.lrange("pushed", 0, -1).join(",")

# a command failing inside EXEC: the others still ran
r.set("str", "x")
begin
  r.multi do |tx|
    tx.rpush("after:1", "a")
    tx.lpush("str", "y")
    tx.rpush("after:2", "b")
  end
  puts "exec_error   none?!"
rescue Redis::CommandError => e
  puts "exec_error   " + e.is_a?(Redis::WrongTypeError).to_s
end
puts "exec_ran     " + r.llen("after:1").to_s + " " + r.llen("after:2").to_s
puts "exec_insync  " + r.ping

# a command the server won't queue: nothing ran
begin
  r.multi do |tx|
    tx.rpush("never", "a")
    tx.hset("h", {})
  end
  puts "queue_error  none?!"
rescue Redis::CommandError => e
  puts "queue_error  " + e.message.include?("wrong number of arguments").to_s + " " + e.message.include?("EXECABORT").to_s
end
puts "queue_ran    " + r.exists("never").to_s
puts "queue_insync " + r.ping

# an empty block
res = r.multi do |tx|
end
puts "empty        " + res.length.to_s

# the same commands on their own
puts "set_nx       " + r.set("n", "1", nx: true).to_s + " " + r.set("n", "2", nx: true).to_s + " " + r.get("n").to_s
puts "set_ex       " + r.set("e", "1", ex: 100).to_s + " " + (r.ttl("e") > 90).to_s
puts "unlink       " + r.unlink(["n", "e", "missing"]).to_s + " " + r.unlink("n").to_s
puts "hset_hash    " + r.hset("hh", {"a" => "1", "b" => "2"}).to_s + " " + r.hset("hh", {"a" => "3", "c" => "4"}).to_s + " " + r.hget("hh", "a").to_s
begin
  r.hset("hs", "f")
  puts "hset_short   none?!"
rescue Redis::CommandError => e
  puts "hset_short   " + e.message.include?("wrong number of arguments").to_s + " " + r.exists("hs").to_s
end
puts "lpush_many   " + r.lpush("lp", ["1", "2"]).to_s + " " + r.lrange("lp", 0, -1).join(",")
puts "sadd_many    " + r.sadd("sm", ["a", "b", "a"]).to_s

# SSCAN over a set larger than one page
i = 0
members = []
while i < 300
  members.push("m" + i.to_s)
  i = i + 1
end
r.sadd("big", members)
seen = {}
r.sscan_each("big", count: 20) do |m|
  seen[m] = true
end
puts "sscan_all    " + seen.length.to_s
n = 0
r.sscan_each("big", match: "m1?") do |m|
  n = n + 1
end
puts "sscan_match  " + n.to_s
n = 0
r.sscan_each("nothing") do |m|
  n = n + 1
end
puts "sscan_none   " + n.to_s

MultiShell.sp_net_shell_capture("redis-cli -p 16434 shutdown nosave 2>/dev/null", 64)
puts "done"
