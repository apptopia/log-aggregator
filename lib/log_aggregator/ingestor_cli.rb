ingestor = LogAggregator::Ingestor.new
logger = Logger.new(LogAggregator.root.join('log/errors.log'))

while line = STDIN.gets
  begin
    line.chomp!
    ingestor.handle_input_line(line)
  rescue
    logger.error "Error #{$!.inspect} (while processing '#{line}'):\n#{$!.backtrace.join "\n"}"
  end
end
