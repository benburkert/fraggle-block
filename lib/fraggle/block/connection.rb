require 'fraggle/block/msg.pb'
require 'socket'
require "system_timer"

module Fraggle
  module Block
    class Connection
      attr_accessor :host, :port, :sock

      def initialize(host, port)
        @host = host
        @port = port
        @sock = connect
      end

      def address
        "#{@host}:#{@port}"
      end

      def connect
        SystemTimer.timeout_after(10) do
          s = TCPSocket.new(@host, @port)
          s.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1
          s
        end
      end

      def disconnect
        @sock.close
      end

      def send(req)
        data = req.encode
        head = [data.length].pack("N")
        @sock.write(head+data)
      end

      def read
        responses = []
        head = @sock.read(4)
        length = head.unpack("N")[0]
        data = @sock.read(length)
        response = Response.decode(data)
        responses << response if response.ok?
        responses
      end
    end
  end
end
