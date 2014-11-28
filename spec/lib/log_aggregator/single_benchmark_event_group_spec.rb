require 'spec_helper'

describe LogAggregator::SingleBenchmarkEventGroup do
  before(:each) {
    LogAggregator.redis.flushdb
  }

  let(:event_group) {LogAggregator::SingleBenchmarkEventGroup.new('cql')}
  let(:t0) {Time.parse('2014-11-28 10:01:00')}

  it "aggregates benchmarks" do
    event_group.register_event('xyz', t0 + 00, 0.150)
    event_group.register_event('xyz', t0 + 60, 0.250)

    event_group.register_error_event('xyz', t0 + 10)

    event_group.register_event('abc', t0 + 00, 0.1)
    event_group.register_event('abc', t0 + 10, 0.2)
    event_group.register_event('abc', t0 + 20, 0.3)
    event_group.register_event('abc', t0 + 60, 0.1)
    event_group.register_event('abc', t0 + 70, 0.4)

    labels, counts, avgs, errors = event_group.minute_series(t0, t0 + 120)

    expect(labels).to eq(["2014-11-28 10:01:00", "2014-11-28 10:02:00", "2014-11-28 10:03:00"])

    expect(counts).to eq({"xyz"=>[1, 1, 0], "abc"=>[3, 2, 0]})

    expect(avgs["xyz"]).to eq([0.150, 0.250, 0])

    a1, a2, a3 = avgs["abc"]
    expect(a1).to be_within(0.0001).of(0.2)
    expect(a2).to be_within(0.0001).of(0.25)
    expect(a3).to be_within(0.0001).of(0.0)

    expect(errors).to eq({"xyz" => [1, 0, 0]})
  end
end
