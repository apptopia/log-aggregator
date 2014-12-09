require 'optparse'

class ConfigError < RuntimeError; end

DEFAULT_INTERVAL = 10
DEFAULT_LIMIT = 20
BENCHMARKS = ['CQL', 'WORKER', 'HTTP']

def parse_time_settings(time_start, time_end, last_minutes)
  raise ConfigError, "Can't use --last-minutes option with any of --time-start, --time-end" if (time_start || time_end) && last_minutes

  if time_start || time_end
    t_time_start = time_start && Time.parse(time_start)
    t_time_end = time_end && Time.parse(time_end)

    t_time_end = t_time_start + DEFAULT_INTERVAL * 60 if t_time_start && !t_time_end
    t_time_start = t_time_end - DEFAULT_INTERVAL * 60 if t_time_end && !t_time_start

    raise ConfigError, "--time-start value should be the same or before before --time-end" if t_time_start > t_time_end

    return t_time_start, t_time_end
  end

  last_minutes ||= DEFAULT_INTERVAL
  raise ConfigError, "--last-minutes should be > 0" unless last_minutes.to_i > 0
  t_time_end = Time.now
  t_time_start = t_time_end - last_minutes.to_i * 60

  return t_time_start, t_time_end
end

def format_time(t)
  t.strftime('%Y-%m-%d %H:%M')
end

def report_hottest(benchmark, t0, t1, limit)
  puts "Running report for HOTTEST queries within #{format_time t0} .. #{format_time t1}"
  puts
  LogAggregator::Reporter.new.send("#{benchmark.downcase}_print_hottest", t0, t1, limit)
end

def report_longest(benchmark, t0, t1, limit)
  puts "Running report for LONGEST queries within #{format_time t0} .. #{format_time t1}"
  puts
  LogAggregator::Reporter.new.send("#{benchmark.downcase}_print_longest", t0, t1, limit)
end

benchmark = BENCHMARKS.first
time_start = nil
time_end = nil
last_minutes = nil
limit = nil

opt_parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{File.basename $0} [options] <command>"

  opts.separator ""
  opts.separator "Commands:"
  opts.separator "       hottest - report most frequently run queries"
  opts.separator "       longest - report longest queries"

  opts.separator ""
  opts.separator "Options:"

  opts.on("-h", "--help", "This help screen") {puts opts; exit(0)}
  opts.on("-b", "--benchmark B", "Selects benchmark: CQL (default), WORKER, HTTP") {|b| benchmark = b.upcase}
  opts.on("--time-start TIME", "Start time for report") {|t| time_start = t}
  opts.on("--time-end TIME", "End time for report") {|t| time_end = t}
  opts.on("--last X", "Run report for time interval of last X minutes") {|t| last_minutes = t}
  opts.on("--top X", "Limit number of reported queries, default is 20") {|x| limit = x.to_i}

  opts.separator ""
  opts.separator "When no time specific option is given, the default is to use last 10 minutes interval."
end

begin
  if ARGV.empty?
    puts opt_parser
    exit(-1)
  end

  opt_parser.parse!(ARGV)
  if ARGV.size != 1
    puts opt_parser
    exit(-1)
  end

  raise ConfigError, "benchmark should be any of #{BENCHMARKS.join ', '}, got #{benchmark}" unless BENCHMARKS.include?(benchmark)

  t0, t1 = parse_time_settings(time_start, time_end, last_minutes)

  limit ||= DEFAULT_LIMIT
  raise ConfigError, "--top value should be greater than 0" if limit <= 0

  case ARGV.first
  when 'hottest'
    report_hottest(benchmark, t0, t1, limit)
  when 'longest'
    report_longest(benchmark, t0, t1, limit)
  else
    puts opt_parser
    exit(-1)
  end

rescue OptionParser::ParseError => e
  puts "Error: #{e.message}"
  puts opt_parser
  exit(-1)
rescue ConfigError => e
  puts "Configuration error: #{e.message}"
  exit(-1)
end
