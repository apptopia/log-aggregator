class LogAggregator::SingleBenchmarkEventGroup
  extend Forwardable

  attr_reader :collection_name

  def initialize(collection_name)
    @collection_name = collection_name
  end

  def register_event(name, timestamp, bm)
    keys = keys_slice('ok', timestamp)
    redis.pipelined do
      keys.each {|k| increment_count(name, k)}
      keys.each {|k| increment_benchmark(name, k, bm)}
      keys.each {|k| increment_overall_count(k)}
      keys.each {|k| increment_overall_becnhmark(k, bm)}
    end
  end

  def register_error_event(name, timestamp)
    keys = keys_slice('error', timestamp)
    redis.pipelined do
      keys.each {|k| increment_count(name, k)}
      keys.each {|k| increment_overall_count(k)}
    end
  end

  def minute_series(time_from, time_to)
    labels = []
    counts = {}
    avgs   = {}
    errors = {}

    overall_counts = []
    overall_avgs   = []
    overall_errors = []

    t = time_from
    while t <= time_to
      label = t.strftime('%Y-%m-%d %H:%M:00')
      labels << label

      ok_key = keys_slice('ok', t)[2]
      error_key = keys_slice('error', t)[2]

      ok_counts = get_counts(ok_key)
      ok_bms = get_benchmarks(ok_key)
      error_counts = get_counts(error_key)

      ok_counts.each {|k, v|
        bm = (ok_bms[k] || 0.0).to_f
        c  = v.to_i

        (counts[k] ||= {})[label] = c
        (avgs[k] ||= {})[label] = bm / c
      }

      error_counts.each {|k, v|
        (errors[k] ||= {})[label] = v.to_i
      }

      overall_ok_count = get_overall_count(ok_key)
      overall_bm = get_overall_becnhmark(ok_key)
      overall_error_count = get_overall_count(error_key)

      overall_counts << overall_ok_count.to_i
      overall_avgs   << (overall_ok_count.to_i > 0 ? ((overall_bm || 0.0).to_f / overall_ok_count.to_i) : 0.0)
      overall_errors << overall_error_count.to_i

      t += 60
    end

    return labels,
      extract_backfilled_series(labels, counts),
      extract_backfilled_series(labels, avgs),
      extract_backfilled_series(labels, errors),
      overall_counts,
      overall_avgs,
      overall_errors
  end

  def extract_backfilled_series(labels, series_hash)
    series = {}

    series_hash.each {|k, s|
      series[k] = labels.map {|l| s[l] || 0}
    }

    series
  end

  def keys_slice(kind, timestamp)
    timestamp = timestamp.clone.utc
    day      = "#{timestamp.to_date.to_s}"
    k_day    = "#{day}/#{kind}"
    k_hour   = "#{day}/#{timestamp.hour}/#{kind}"
    k_minute = "#{day}/#{timestamp.hour}:#{timestamp.min}/#{kind}"

    [k_day, k_hour, k_minute]
  end

  def get_counts(collection)
    redis.hgetall("#{collection_name}/counts/#{collection}")
  end

  def get_benchmarks(collection)
    redis.hgetall("#{collection_name}/durations/#{collection}")
  end

  def get_overall_count(collection)
    redis.get("#{collection_name}/overall_count/#{collection}")
  end

  def get_overall_becnhmark(collection)
    redis.get("#{collection_name}/overall_duration/#{collection}")
  end

  def increment_overall_count(collection)
    redis.incr("#{collection_name}/overall_count/#{collection}")
  end

  def increment_overall_becnhmark(collection, bm)
    redis.incrbyfloat("#{collection_name}/overall_duration/#{collection}", bm)
  end

  def increment_count(name, collection)
    redis.hincrby("#{collection_name}/counts/#{collection}", name, 1)
  end

  def increment_benchmark(name, collection, f)
    redis.hincrbyfloat("#{collection_name}/durations/#{collection}", name, f)
  end

  def_delegator ::LogAggregator, :redis
end
