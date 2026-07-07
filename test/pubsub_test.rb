# SUBSCRIBE-mode dispatch conformance against a scripted transport —
# dual-runtime (no snapshot). Push-array shapes come straight from the
# RESP2 spec: confirmations carry the new count, messages carry bulk
# (binary-safe) payloads, and the loop exits when the count returns to 0.
require "redis/resp"
require "redis/pubsub"

class ScriptedTransport
  def initialize(chunks)
    @chunks = chunks
    @i = 0
    @written = ""
  end

  def write(data)
    @written = @written + data
    data.bytesize
  end

  def read_some(max)
    if @i >= @chunks.length
      return ""
    end
    c = @chunks[@i]
    @i = @i + 1
    c
  end

  def close
    0
  end

  def written
    @written
  end
end

def push3(kind, a, b)
  "*3\r\n$" + kind.bytesize.to_s + "\r\n" + kind + "\r\n$" + a.bytesize.to_s + "\r\n" + a + "\r\n" + b + "\r\n"
end

def conf(kind, name, count)
  push3(kind, name, ":" + count.to_s)
end

def msg(channel, payload)
  push3("message", channel, "$" + payload.bytesize.to_s + "\r\n" + payload)
end

# --- subscribe: confirmation, two messages, handler-driven unsubscribe ----

script = [
  conf("subscribe", "news", 1),
  msg("news", "m1"),
  msg("news", "m2"),
  conf("unsubscribe", "news", 0)
]
t = ScriptedTransport.new(script)
ps = RedisPubSub.new(t)
events = []
ps.subscribe("news") do |on|
  on.subscribe do |ch, n|
    events.push("sub:" + ch + ":" + n.to_s)
  end
  on.message do |ch, m|
    events.push("msg:" + ch + ":" + m)
    if m == "m2"
      ps.unsubscribe("news")
    end
  end
  on.unsubscribe do |ch, n|
    events.push("unsub:" + ch + ":" + n.to_s)
  end
end
puts "events       " + events.join(",")
puts "loop_exited  " + (ps.subscription_count == 0).to_s
w = t.written
puts "sub_wire     " + w.include?("*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n").to_s
puts "unsub_wire   " + w.include?("*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\nnews\r\n").to_s

# --- binary payload: NUL and CRLF bytes survive dispatch -------------------

bin = "a" + [0].pack("C*") + "\r\nb"
script = [
  conf("subscribe", "raw", 1),
  msg("raw", bin),
  conf("unsubscribe", "raw", 0)
]
t = ScriptedTransport.new(script)
ps = RedisPubSub.new(t)
bin_ok = false
ps.subscribe("raw") do |on|
  on.message do |ch, m|
    ok = m.bytesize == 5
    ok = ok && m.getbyte(1) == 0
    bin_ok = ok
    ps.unsubscribe_all
  end
end
puts "binary_msg   " + bin_ok.to_s

# --- messages split across reads reassemble before dispatch ----------------

whole = conf("subscribe", "slow", 1) + msg("slow", "payload-one") + conf("unsubscribe", "slow", 0)
chunks = []
i = 0
while i < whole.bytesize
  chunks.push(whole.byteslice(i, 7))
  i = i + 7
end
t = ScriptedTransport.new(chunks)
ps = RedisPubSub.new(t)
got = ""
ps.subscribe("slow") do |on|
  on.message do |ch, m|
    got = m
    ps.unsubscribe_all
  end
end
puts "chunked_msg  " + (got == "payload-one").to_s

# --- psubscribe: pmessage carries pattern + concrete channel ---------------

pm = "*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$8\r\nnews.abc\r\n$5\r\nhello\r\n"
script = [
  conf("psubscribe", "news.*", 1),
  pm,
  conf("punsubscribe", "news.*", 0)
]
t = ScriptedTransport.new(script)
ps = RedisPubSub.new(t)
pevents = []
ps.psubscribe("news.*") do |on|
  on.psubscribe do |pat, n|
    pevents.push("psub:" + pat)
  end
  on.pmessage do |pat, ch, m|
    pevents.push("pmsg:" + pat + ":" + ch + ":" + m)
    ps.punsubscribe_all
  end
  on.punsubscribe do |pat, n|
    pevents.push("punsub:" + pat + ":" + n.to_s)
  end
end
puts "p_events     " + pevents.join(",")

# --- subscribe_many: two channels, loop runs until BOTH unsubscribe --------

script = [
  conf("subscribe", "a", 1),
  conf("subscribe", "b", 2),
  msg("b", "from-b"),
  conf("unsubscribe", "a", 1),
  conf("unsubscribe", "b", 0)
]
t = ScriptedTransport.new(script)
ps = RedisPubSub.new(t)
many = []
ps.subscribe_many(["a", "b"]) do |on|
  on.subscribe do |ch, n|
    many.push("sub:" + ch + ":" + n.to_s)
  end
  on.message do |ch, m|
    many.push("msg:" + ch)
    ps.unsubscribe_all
  end
  on.unsubscribe do |ch, n|
    many.push("unsub:" + ch + ":" + n.to_s)
  end
end
puts "many_events  " + many.join(",")
w = t.written
puts "many_wire    " + w.include?("*3\r\n$9\r\nSUBSCRIBE\r\n$1\r\na\r\n$1\r\nb\r\n").to_s

# --- connection dropped mid-subscribe raises --------------------------------

script = [conf("subscribe", "dead", 1)]
t = ScriptedTransport.new(script)
ps = RedisPubSub.new(t)
raised = false
begin
  ps.subscribe("dead") do |on|
    on.message do |ch, m|
    end
  end
rescue => e
  raised = e.message.include?("connection lost")
end
puts "lost_raises  " + raised.to_s

# --- drain surface: subscribe_start + drain against scripted chunks --------

conf_bytes = conf("subscribe", "d", 1)
half = conf_bytes.byteslice(0, 5)
rest = conf_bytes.byteslice(5, conf_bytes.bytesize - 5)
script = [
  half,                                   # drain 1: partial confirmation
  rest + msg("d", "m1"),                  # drain 2: completes conf + one msg
  msg("d", "m2") + conf("unsubscribe", "d", 0)   # drain 3: msg + unsub
]
t = ScriptedTransport.new(script)
ps = RedisPubSub.new(t)
devents = []
dl = RedisListener.new
dl.subscribe do |ch, n|
  devents.push("sub:" + ch)
end
dl.message do |ch, m|
  devents.push("msg:" + m)
end
dl.unsubscribe do |ch, n|
  devents.push("unsub:" + n.to_s)
end
ps.subscribe_start("d")
w = t.written
puts "start_wire   " + w.include?("*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nd\r\n").to_s
n1 = ps.drain(dl)
n2 = ps.drain(dl)
n3 = ps.drain(dl)
puts "drain_counts " + n1.to_s + n2.to_s + n3.to_s
puts "drain_events " + devents.join(",")
puts "drain_count0 " + (ps.subscription_count == 0).to_s
