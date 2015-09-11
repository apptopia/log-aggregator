require 'active_support/core_ext/hash/slice'

class LogAggregator::MeasurementSeries
  extend Forwardable

  attr_reader :series

  def initialize(series)
    @series = series
  end

  def register_event(tags, values)
    influxdb.write_point(series, {tags: tags, values: values})
  end

  def_delegator ::LogAggregator, :influxdb
end
