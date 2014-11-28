require 'spec_helper'

# TODO fix: timestamps are TZ sensitive, pull ActiveSupport as another dep?

describe LogAggregator::Ingestor do
  before(:each) {
    LogAggregator.redis.flushdb
  }

  let(:sample_lines) {
    <<-TXT
Nov 27 09:50:52 prod-boglach-worker12 app-boglach[9054]: 09:50:52.716 :83222820 # #CQL :: {"type":"legend","key":"abc","query":"SELECT LOL"}
Nov 27 09:50:52 prod-boglach-worker12 app-boglach[9054]: 09:50:52.716 :83222820 # #CQL :: {"type":"legend","key":"xyz","query":"INSERT FOO"}
Nov 27 09:50:52 prod-boglach-worker12 app-boglach[9054]: 09:50:52.716 :83222820 # #CQL :: {"type":"query","query":"abc","ts":1417099852,"t":0.009576955}
Nov 27 09:50:52 prod-boglach-worker12 app-boglach[9054]: 09:50:52.732 :83222820 # #CQL :: {"type":"query","query":"abc","ts":1417099852,"t":0.014291932}
Nov 27 09:50:52 prod-boglach-worker12 app-boglach[9054]: 09:50:52.738 :83222820 # #CQL :: {"type":"query","query":"abc","ts":1417099952,"t":0.005578929}
Nov 27 09:50:52 prod-boglach-worker12 app-boglach[8808]: 09:50:52.739 :69180140 # #CQL :: {"type":"query","query":"xyz","ts":1417099852,"t":0.041778716}
Nov 27 09:50:52 prod-boglach-worker12 app-boglach[8808]: 09:50:52.744 :69180140 # #CQL :: {"type":"query","query":"xyz","ts":1417099852,"t":0.002873777}
Nov 27 09:50:52 prod-boglach-worker12 app-boglach[8808]: 09:50:52.751 :81343100 # #CQL :: {"type":"query","query":"xyz","ts":1417099952,"t":0.006966371}
Nov 27 09:50:52 prod-boglach-worker12 app-boglach[8808]: 09:50:52.753 :69180140 # #CQL :: {"type":"query","query":"xyz","ts":1417099952,"t":0.009008195}
    TXT
  }

  let(:cql_line) {
    'Nov 27 09:50:52 prod-boglach-worker12 app-boglach[8808]: 09:50:52.753 :69180140 # #CQL :: {"type":"query","query":"xyz","ts":1417099952,"t":0.009008195}'
  }

  let(:worker_bm_line) {
    'Nov 27 09:52:35 prod-boglach-worker21 app-boglach[9004]: 09:52:35.159 :92887440 # #WORKER-BM {"ts":1417099951,"worker":"Canonic::GooglePlay::AppBreakoutCalculationsWorker","queue":"google_play_app_breakout_calculations","total":3.3580825328826904,"other":1.4104158228826902,"cql":1.9476667100000002,"s3":0.0,"sql":0}'
  }

  let(:irrelevant_line) {
    'Nov 27 09:52:35 prod-boglach-worker21 app-boglach[9004]: 09:52:35.159 :92887440 # #BOO {}'
  }

  let(:ingestor) {LogAggregator::Ingestor.new}

  it "handles log lines" do
    sample_lines.each_line {|l|
      ingestor.handle_logline('CQL', l)
    }

    labels, counts, avgs, errors = ingestor.cql_benchmark.minute_series(
      Time.parse('2014-11-27 16:50:00'),
      Time.parse('2014-11-27 16:52:00'),
    )

    expect(labels).to eq(["2014-11-27 16:50:00", "2014-11-27 16:51:00", "2014-11-27 16:52:00"])

    expect(counts).to eq({"xyz"=>[2, 0, 2], "abc"=>[2, 0, 1]})

    a1, a2, a3 = avgs["abc"]
    expect(a1).to be_within(0.0001).of(0.0119)
    expect(a2).to be_within(0.0001).of(0.0)
    expect(a3).to be_within(0.0001).of(0.0055)
  end

  it "recognizes CQL tagged lines" do
    expect(ingestor).to receive(:handle_logline).with('CQL', cql_line)
    ingestor.handle_input_line(cql_line)
  end

  it "recognizes WORKER-BM tagged lines" do
    expect(ingestor).to receive(:handle_logline).with('WORKER-BM', worker_bm_line)
    ingestor.handle_input_line(worker_bm_line)
  end

  it "ignores irrelevant lines" do
    expect(ingestor).not_to receive(:handle_logline)
    ingestor.handle_input_line(irrelevant_line)
  end
end
