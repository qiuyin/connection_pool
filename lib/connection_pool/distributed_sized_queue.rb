require 'thread'
require 'timeout'
require 'redlock'
require 'redis'
require 'byebug'
require 'securerandom'
require_relative "pool_shutting_down_error"

##
# The DistributedSizedQueue manages a pool of homogeneous connections (or any resource
# you wish to manage).  Connections are created lazily up to a given maximum
# number.

# Examples:
#
#    ts = DistributedSizedQueue.new(1) { MyConnection.new }
#
#    # fetch a connection
#    conn = ts.pop
#
#    # return a connection
#    ts.push conn
#
#    conn = ts.pop
#    ts.pop timeout: 5
#    #=> raises Timeout::Error after 5 seconds

class ConnectionPool::DistributedSizedQueue
  attr_reader :max

  ##
  # Creates a new pool with +size+ connections that are created from the given
  # +block+.

  def initialize(size = 0, &block)
    @redis_url = 'redis://127.0.0.1:6379'
    @create_block = block
    @que = []
    @max = size
    @lock_manager = ::Redlock::Client.new([ @redis_url ])
    @redis = ::Redis.new(url: @redis_url)
    @resource = 'resource' # change to param
    @shutdown_block = nil
    @lock_key = "lock_key" # change to param
  end

  ##
  # Returns +obj+ to the stack.  +options+ is ignored in DistributedSizedQueue but may be
  # used by subclasses that extend DistributedSizedQueue.

  def push(obj, options = {})
    loop do
      @lock_manager.lock(@lock_key, 2000) do |locked|
        if locked
          if @shutdown_block
            @shutdown_block.call(obj)
          else
            store_connection obj, options
          end

          @redis.publish(@resource, 'Back one resource to pool')
          return
        else
          sleep 1
        end
      end
    end
  end
  alias_method :<<, :push

  ##
  # Retrieves a connection from the stack.  If a connection is available it is
  # immediately returned.  If no connection is available within the given
  # timeout a Timeout::Error is raised.
  #
  # +:timeout+ is the only checked entry in +options+ and is preferred over
  # the +timeout+ argument (which will be removed in a future release).  Other
  # options may be used by subclasses that extend DistributedSizedQueue.

  def pop(timeout = 0.5, options = {})
    options, timeout = timeout, 0.5 if Hash === timeout
    timeout = options.fetch :timeout, timeout

    deadline = ::Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      @lock_manager.lock(@lock_key, 2000) do |locked|
        if locked
          raise ConnectionPool::PoolShuttingDownError if @shutdown_block
          return fetch_connection(options) if connection_stored?(options)

          connection = try_create(options)
          return connection if connection
        end
      end

      to_wait = deadline - ::Process.clock_gettime(Process::CLOCK_MONOTONIC)
      raise Timeout::Error, "Waited #{timeout} sec" if to_wait <= 0

      @redis.subscribe_with_timeout(to_wait, @resource){ |on| on.message { |channel, message| @redis.unsubscribe }}
    end
  end

  ##
  # Shuts down the DistributedSizedQueue which prevents connections from being checked
  # out.  The +block+ is called once for each connection on the stack.

  def shutdown(&block)
    raise ArgumentError, "shutdown must receive a block" unless block_given?

    loop do
      @lock_manager.lock(@lock_key, 2000) do |locked|
        if locked
          @shutdown_block = block
          @redis.publish(@resource, 'Back one resource to pool')

          shutdown_connections
          break
        end
      end
    end
  end

  ##
  # Returns +true+ if there are no available connections.

  def empty?
    created >= @max
  end

  ##
  # The number of connections available on the stack.

  def length
    @max - created
  end

  private

  ##
  # This is an extension point for DistributedSizedQueue and is called with a mutex.
  #
  # This method must returns true if a connection is available on the stack.

  def connection_stored?(options = nil)
    !@que.empty?
  end

  ##
  # This is an extension point for DistributedSizedQueue and is called with a mutex.
  #
  # This method must return a connection from the stack.

  def fetch_connection(options = nil)
    @que.pop
  end

  ##
  # This is an extension point for DistributedSizedQueue and is called with a mutex.
  #
  # This method must shut down all connections on the stack.

  def shutdown_connections(options = nil)
    while connection_stored?(options)
      conn = fetch_connection(options)
      @shutdown_block.call(conn)
    end
  end

  ##
  # This is an extension point for DistributedSizedQueue and is called with a mutex.
  #
  # This method must return +obj+ to the stack.

  def store_connection(obj, options = nil)
    @que.push obj
    @redis.decr 'created' if created > 0
  end

  ##
  # This is an extension point for DistributedSizedQueue and is called with a mutex.
  #
  # This method must create a connection if and only if the total number of
  # connections allowed has not been met.

  def try_create(options = nil)
    unless created == @max
      object = @create_block.call
      @redis.incr 'created'
      object
    end
  end

  def created
    (@redis.get 'created').to_i
  end
end
