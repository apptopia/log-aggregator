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

    # FIXME: write data somewhere
  end
end
