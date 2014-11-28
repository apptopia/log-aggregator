class LogAggregator::Reporter
  attr_reader :cql_legend
  attr_reader :cql_benchmark

  def initialize
    @cql_legend = LogAggregator::QueryMap.new('cql_legend')
    @cql_benchmark = LogAggregator::SingleBenchmarkEventGroup.new('cql')
  end

  def print_longest(time_from, time_to, limit)
    legend, counts, avgs, errors = self.cql_benchmark.minute_series(time_from, time_to)
    sorted = avgs.sort_by {|q, series| -series.max}.take(limit).map(&:first)
    print_table(sorted, legend, counts, avgs, errors)
  end

  def print_hottest(time_from, time_to, limit)
    legend, counts, avgs, errors = self.cql_benchmark.minute_series(time_from, time_to)
    sorted = counts.sort_by {|q, series| -series.inject(0) {|s, c| s + c}}.take(limit).map(&:first)
    print_table(sorted, legend, counts, avgs, errors)
  end

  def print_table(qs, legend, counts, avgs, errors)
    qs.each {|q|
      query = self.cql_legend.get(q) || 'N/A'
      c_series = counts[q] || []
      a_series = avgs[q]   || []
      e_series = errors[q] || []

      printf "Query: %s\n\n", query

      printf "%20s %10s %10s %10s\n", 'Timestamp', 'Count', 'Average', 'Errors'

      legend.each_with_index {|l, i|
        printf "%20s %10d %10.3f %10d\n", l, c_series[i] || 0, a_series[i] || 0.0, e_series[i] || 0
      }
    }
  end

end
