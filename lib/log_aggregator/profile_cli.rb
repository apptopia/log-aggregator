require 'ruby-prof'

def profile_ingest(file, sample_lines_count)
  File.open(file, 'r') {|f|
    i = LogAggregator::Ingestor.new

    # warm up
    10.times {
      line = f.gets
      raise "Too few lines in #{file}!" unless line
      line.chomp!
      i.handle_input_line(line)
    }

    RubyProf.start
    sample_lines_count.times {
      line = f.gets
      raise "Too few lines in #{file}!" unless line
      line.chomp!
      i.handle_input_line(line)
    }

    result = RubyProf.stop
    printer = RubyProf::GraphHtmlPrinter.new(result)
    File.open("graph.html", 'w') {|f| printer.print(f)}
    puts "Graph profile saved to 'graph.html'"
  }
end

if ARGV.size != 1
  puts "Usage: #{File.basename $0} <log_sample_file>"
  exit(-1)
end

profile_ingest(ARGV[0], 5000)
