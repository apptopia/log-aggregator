class LogAggregator::QueryMap
  extend Forwardable

  attr_reader :collection_name

  def initialize(collection_name)
    @collection_name = collection_name
  end

  def update(key, value)
    redis.hset(collection_key, key, value)
  end

  def get(key)
    mget([key]).first
  end

  def mget(keys)
    values = redis.hmget(collection_key, *keys)
    hash = {}
    keys.each_with_index {|k, i| hash[k] = values[i]}

    hash
  end

  def collection_key
    "query_maps:#{collection_name}"
  end

  def_delegator ::LogAggregator, :redis
end
