class LogAggregator::Reporter
  attr_reader :cql_legend
  attr_reader :cql_benchmark
  attr_reader :worker_benchmark

  def initialize
    @cql_legend = LogAggregator::QueryMap.new('cql_legend')
    @cql_benchmark = LogAggregator::SingleBenchmarkEventGroup.new('cql')
    @worker_benchmark = LogAggregator::SingleBenchmarkEventGroup.new('worker')
  end

  def cql_print_longest(time_from, time_to, limit)
    print_longest(self.cql_benchmark, time_from, time_to, limit, 'Query', method(:translate_cql_key))
  end

  def cql_print_hottest(time_from, time_to, limit)
    print_hottest(self.cql_benchmark, time_from, time_to, limit, 'Query', method(:translate_cql_key))
  end

  def print_longest(benchmark, time_from, time_to, limit, title, translate_key_proc = nil)
    legend, counts, avgs, errors, overall_counts, overall_avgs, overall_errors = benchmark.minute_series(time_from, time_to)
    sorted = avgs.sort_by {|q, series| -series.max}.take(limit).map(&:first)
    print_overall(legend, overall_counts, overall_avgs, overall_errors)
    print_table(sorted, legend, counts, avgs, errors, title, translate_key_proc)
  end

  def print_hottest(benchmark, time_from, time_to, limit, title, translate_key_proc = nil)
    legend, counts, avgs, errors, overall_counts, overall_avgs, overall_errors = benchmark.minute_series(time_from, time_to)
    sorted = counts.sort_by {|q, series| -series.inject(0) {|s, c| s + c}}.take(limit).map(&:first)
    print_overall(legend, overall_counts, overall_avgs, overall_errors)
    print_table(sorted, legend, counts, avgs, errors, title, translate_key_proc)
  end

  def print_overall(legend, counts, avgs, errors)
    printf "OVERALL STATS\n\n"

    printf "%20s %10s %10s %10s\n", 'Timestamp', 'Count', 'Average', 'Errors'

    legend.each_with_index {|l, i|
      printf "%20s %10d %10.3f %10d\n", l, counts[i] || 0, avgs[i] || 0.0, errors[i] || 0
    }
  end

  def translate_cql_key(key)
    self.cql_legend.get(key) || 'N/A'
  end

  def print_table(qs, legend, counts, avgs, errors, key_name, translate_key_proc = nil)
    qs.each {|q|
      printf "\n"
      key = translate_key_proc ? translate_key_proc.call(q) : q
      c_series = counts[q] || []
      a_series = avgs[q]   || []
      e_series = errors[q] || []

      printf "%s: %s\n\n", key_name, key

      printf "%20s %10s %10s %10s\n", 'Timestamp', 'Count', 'Average', 'Errors'

      legend.each_with_index {|l, i|
        printf "%20s %10d %10.3f %10d\n", l, c_series[i] || 0, a_series[i] || 0.0, e_series[i] || 0
      }
    }
  end

end
