# frozen_string_literal: true

##
# Raised when you attempt to retrieve a connection from a pool that has been
# shut down.

class ConnectionPool::PoolShuttingDownError < RuntimeError; end