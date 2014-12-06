class LogAggregator::Ingestor
  attr_reader :cql_legend
  attr_reader :cql_benchmark
  attr_reader :worker_benchmark

  def initialize
    @cql_legend = LogAggregator::QueryMap.new('cql_legend')
    @cql_benchmark = LogAggregator::SingleBenchmarkEventGroup.new('cql')
    @worker_benchmark = LogAggregator::SingleBenchmarkEventGroup.new('worker')
  end

  TAGS_REGEX = /#(CQL|HTTP-BM|WORKER-BM)/

  def handle_input_line(line)
    m = TAGS_REGEX.match(line)
    handle_logline(m[1], line) if m
  end

  def handle_logline(tag, line)
    _, json_str = line.split(' :: ')
    json_str.chomp!
    json_str.strip!
    handle_event(tag, JSON.parse(json_str))
  end

  def handle_event(tag, event)
    case tag
    when 'CQL'
      handle_cql_event(event)
    when 'WORKER-BM'
      handle_worker_event(event)
    else
      # ignore
    end
  end

  def handle_cql_event(event)
    case event['type']
    when 'legend'
      self.cql_legend.update(event['key'], event['query'])
    when 'query'
      ts = Time.at(event['ts'])
      bm = event['t']

      if !event['error']
        self.cql_benchmark.register_event(event['query'], ts, bm)
      else
        self.cql_benchmark.register_error_event(event['query'], ts)
      end
    when 'batch'
      ts = Time.at(event['ts'])
      bm = event['t']

      query_batch = event['query_batch']
      query_batch.each {|q, count|
        count.times {
          if !event['error']
            self.cql_benchmark.register_event(query, ts, bm)
          else
            self.cql_benchmark.register_error_event(query, ts)
          end
        }
      }
    else
      # Ignore the rest
      nil
    end
  end

  def handle_worker_event(event)
    ts = Time.at(event['ts'])
    bm = event['t']
    self.worker_benchmark.register_event(event['worker'], ts, bm)
  end

end
