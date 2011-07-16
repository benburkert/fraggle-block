require 'fraggle/block/connection'

module Fraggle
  module Block
    class Client
      include Request::Verb

      class OutOfNodes < StandardError; end

      attr_accessor :addrs
      attr_reader :connection

      def initialize(addrs = [])
        @addrs, @clock = addrs, 0
        connect
      end

      def rev
        request = Request.new(:tag => tick, :verb => REV)
        send(request).first
      end

      def latest_rev
        rev.rev
      end

      def get(path, rev = latest_rev)
        request = Request.new(:tag => tick, :path => path, :rev => rev, :verb => GET)
        send(request).first
      end

      def getdir(path, offset = 0, rev = latest_rev)
        request = Request.new(:tag => tick, :path => path, :rev => rev, :offset => offset, :verb => GETDIR)
        send(request).first
      end

      def getdir_all(path, rev = latest_rev)
        responses, offset = [], 0

        while(response = walk(path, offset, rev))
          responses << response
          offset += 1
        end

        responses
      end

      def set(path, value, rev = latest_rev)
        request = Request.new(:tag => tick, :path => path, :value => value, :rev => rev, :verb => SET)
        send(request).first
      end

      def del(path, rev = latest_rev)
        request = Request.new(:tag => tick, :path => path, :rev => rev, :verb => DEL)
        send(request).first
      end

      def walk(path, offset = 0, rev = latest_rev)
        request = Request.new(:tag => tick, :path => path, :rev => rev, :offset => offset, :verb => WALK)
        send(request).first
      end

      def walk_all(path, rev = latest_rev)
        responses, offset = [], 0

        while(response = walk(path, offset, rev))
          responses << response
          offset += 1
        end

        responses
      end

      def wait(path, rev = latest_rev)
        request = Request.new(:tag => tick, :path => path, :rev => rev, :verb => WAIT)
        send(request).first
      end

      def disconnect
        @connection.disconnect
      end

      def reconnect
        disconnect
        connect
      end

      def connect
        begin
          host, port = @addrs.shift.split(':')
          @connection = connection_to(host, port)
          find_all_of_the_nodes
        rescue => e
          retry if @addrs.any?
          raise OutOfNodes, "where did they go?"
        end
      end

      def connection_to(host, port)
        Connection.new(host, port)
      end

      def find_all_of_the_nodes
        offset = 0

        while(node = walk('/ctl/node/*/addr', offset))
          @addrs << node.value unless @addrs.include? node.value
          offset += 1
        end
      end

      def to_s
        addrs.join(',')
      end

    protected

      def send(request)
        @connection.send(request)
        @connection.read
      end

      def tick
        @clock += 1
      end
    end
  end
end
