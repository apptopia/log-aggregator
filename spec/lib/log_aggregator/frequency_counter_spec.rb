require 'spec_helper'

describe LogAggregator::FrequencyCounter do
  before(:each) { LogAggregator.redis.flushdb }

  let(:counter) {LogAggregator::FrequencyCounter.new('app_data_processing')}

  it 'counts rate of previously seen events' do
    event_attrs = {'app_id' => 1002393085, 'country_iso' => 'PH', 'date' => '2015-09-15'}

    counter.register_event('itunes_connect', event_attrs, DateTime.parse('2015-09-15T16:13:03+02:00'))

    collection = '2015-09-15/itunes_connect'
    expect(counter.get_overall_count(collection)).to eq(1)
    expect(counter.get_previously_seen_count(collection)).to eq(0)
    expect(counter.get_previously_seen_rate(collection)).to eq(0)

    counter.register_event('itunes_connect', event_attrs, DateTime.parse('2015-09-15T16:13:03+02:00'))

    expect(counter.get_overall_count(collection)).to eq(2)
    expect(counter.get_previously_seen_count(collection)).to eq(1)
    expect(counter.get_previously_seen_rate(collection)).to eq(0.5)
  end
end
