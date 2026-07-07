# The real transport: a TCP connection to a Redis server over sp_net.
# Matches the transport duck RedisClientCore expects (write / read_some /
# close). :binstr on the recv side keeps bulk payloads binary-safe;
# write_bytes takes an explicit byte count so embedded NULs survive the
# send side too.
require "redis/sock"

class RedisTransport
  def initialize(host, port)
    @fd = RedisSock.sp_net_connect(host, port)
    if @fd < 0
      raise "redis: cannot connect to " + host + ":" + port.to_s
    end
  end

  # The raw socket fd — embedders with their own event loop park on
  # this (poll / Tep::Scheduler.io_wait) and then drain.
  def fd
    @fd
  end

  def write(data)
    RedisSock.sp_net_write_bytes(@fd, data, data.bytesize)
  end

  def read_some(max)
    RedisSock.sp_net_recv_some(@fd, max)
  end

  def close
    RedisSock.sp_net_close(@fd)
  end
end
