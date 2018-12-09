# frozen_string_literal: true

require_relative './connection_pool'

class DistributedConnectionPool < ConnectionPool
  def pool_manager(size, &block)
    DistributedSizedQueue.new(size, &block)
  end
end
