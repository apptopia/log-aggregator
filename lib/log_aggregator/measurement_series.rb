require 'active_support/core_ext/hash/slice'
require 'securerandom'

class LogAggregator::MeasurementSeries
  extend Forwardable

  attr_reader :series, :points

  def initialize(series)
    @series = series
    @points = []
  end

  def register_event(tags, values, timestamp)
    tags.merge!(uuid: SecureRandom.uuid)
    points << {series: series, tags: tags, values: values, timestamp: timestamp}
    if points.size > 20
      influxdb.write_points(points)
      points.clear
    end
  end

  def_delegator ::LogAggregator, :influxdb
end
