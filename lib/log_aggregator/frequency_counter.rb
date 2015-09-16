require 'bloomfilter'
require 'active_support/core_ext/date'
require 'active_support/core_ext/date_time'
require 'active_support/core_ext/integer/time'

class LogAggregator::FrequencyCounter
  extend Forwardable

  attr_reader :collection_name

  def initialize(collection_name)
    @collection_name = collection_name
  end

  def register_event(event_attributes, timestamp)
    ts = timestamp.clone.utc
    collection = ts.to_date.to_s

    filter = previously_seen_filter(collection, ts)
    event_key = event_attributes.sort.to_s

    previously_seen_count =
      if filter.include?(event_key)
        increment_previously_seen_count(collection)
      else
        redis.pipelined do
          filter.insert(event_key)
          redis.expireat(filter_key(collection), 1.day.from_now.to_i)
        end
        get_previously_seen_count(collection)
      end

    overall_count = increment_overall_count(collection)
    if overall_count > 0
      previously_seen_rate = previously_seen_count.to_f / overall_count.to_f
      set_previously_seen_rate(collection, previously_seen_rate)
    end
  end

  def increment_previously_seen_count(collection)
    redis.incr(previously_seen_count_key(collection))
  end

  def get_previously_seen_count(collection)
    redis.get(previously_seen_count_key(collection)).to_i
  end

  def set_previously_seen_count(collection, count)
    redis.set(previously_seen_count_key(collection), count)
  end

  def previously_seen_count_key(collection)
    "#{collection_name}/previously_seen_count/#{collection}"
  end

  def increment_overall_count(collection)
    redis.incr(overall_count_key(collection))
  end

  def get_overall_count(collection)
    redis.get(overall_count_key(collection)).to_i
  end

  def overall_count_key(collection)
    "#{collection_name}/overall_count/#{collection}"
  end

  def get_previously_seen_rate(collection)
    redis.get(previously_seen_rate_key(collection)).to_f
  end

  def set_previously_seen_rate(collection, rate)
    redis.set(previously_seen_rate_key(collection), rate)
  end

  def previously_seen_rate_key(collection)
    "#{collection_name}/previously_seen_rate/#{collection}"
  end

  def previously_seen_filter(collection, timestamp)
    # p = 0.0001       --- desired probability of getting false positive
    # n = 30_000_000   --- approx number of scrapings per day
    # m = (n * Math.log(p) / Math.log(1.0 / 2 ** Math.log(2))).ceil
    m = 575_103_503
    # k = (Math.log(2) * m / n).round
    k = 13

    BloomFilter::Redis.new(db: redis, namespace: filter_key(collection),
                           size: m, hashes: k, seed: timestamp.at_midnight.to_i)
  end

  def filter_key(collection)
    "#{collection_name}/previously_seen_filter/#{collection}"
  end

  def_delegator ::LogAggregator, :redis
end
