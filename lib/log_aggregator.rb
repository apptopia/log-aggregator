require 'set'
require 'yaml'
require 'redis'
require 'time'

module LogAggregator
  require_relative 'log_aggregator/query_map'
  require_relative 'log_aggregator/single_benchmark_event_group'

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
