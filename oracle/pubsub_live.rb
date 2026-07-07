# Oracle twin of test/pubsub_live_test.rb (real redis-rb gem). The
# subscription count is tracked from the confirmations themselves —
# redis-rb has no public counter, and that IS the semantic the snapshot
# asserts (the loop exits when the count drains to zero).
require "redis"

port = Integer(ARGV[0] || "6379")
publisher = Redis.new(host: "127.0.0.1", port: port)
subscriber = Redis.new(host: "127.0.0.1", port: port)

count = -1
bin = [112, 0, 13, 10, 113].pack("C*")
subscriber.subscribe("live:chan") do |on|
  on.subscribe do |ch, n|
    puts "subscribed   " + ch + " count=" + n.to_s
    receivers = publisher.publish("live:chan", "m1")
    puts "publish_m1   receivers=" + receivers.to_s
  end
  on.message do |ch, m|
    if m == "m1"
      puts "got_m1       " + ch
      publisher.publish("live:chan", bin)
    else
      ok = m.bytesize == 5
      ok = ok && m.getbyte(1) == 0
      ok = ok && m.getbyte(2) == 13
      puts "got_binary   " + ok.to_s
      subscriber.unsubscribe
    end
  end
  on.unsubscribe do |ch, n|
    puts "unsubscribed " + ch + " count=" + n.to_s
    count = n
  end
end
puts "sub_leg_done count=" + count.to_s

count = -1
subscriber.psubscribe("live:p:*") do |on|
  on.psubscribe do |pat, n|
    puts "psubscribed  " + pat + " count=" + n.to_s
    publisher.publish("live:p:alpha", "pm1")
  end
  on.pmessage do |pat, ch, m|
    puts "got_pmessage " + pat + " " + ch + " " + m
    subscriber.punsubscribe
  end
  on.punsubscribe do |pat, n|
    puts "punsubscribd " + pat + " count=" + n.to_s
    count = n
  end
end
puts "psub_leg_done count=" + count.to_s

puts "publisher_ok " + publisher.ping
subscriber.close
publisher.close
puts "done"
