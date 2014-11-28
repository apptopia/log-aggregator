require 'set'
require 'json'
require 'yaml'
require 'redis'
require 'time'
require 'logger'

module LogAggregator
  require_relative 'log_aggregator/query_map'
  require_relative 'log_aggregator/single_benchmark_event_group'
  require_relative 'log_aggregator/ingestor'
  require_relative 'log_aggregator/reporter'

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
