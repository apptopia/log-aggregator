require 'active_support/core_ext/hash/slice'
require 'securerandom'

class LogAggregator::MeasurementSeries
  extend Forwardable

  attr_reader :series

  def initialize(series)
    @series = series
  end

  def register_event(tags, values, timestamp)
    tags.merge!(uuid: SecureRandom.uuid)
    influxdb.write_point(series, {tags: tags, values: values, timestamp: timestamp}, 's')
  end

  def_delegator ::LogAggregator, :influxdb
end
