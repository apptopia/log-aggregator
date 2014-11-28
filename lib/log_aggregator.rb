require 'yaml'
require 'redis'

module LogAggregator
  require_relative 'log_aggregator/query_map'

  def self.root
    @root ||= Pathname.new(File.expand_path('../../', __FILE__))
  end

  def self.redis
    @redis ||= begin
      redis_config = YAML.load_file(root.join('config/redis.yml'))
      host = redis_config['host']
      port = redis_config['port']
      Redis.new(:host => host, :port => port)
    end
  end
end
