require 'spec_helper'

describe LogAggregator::Ingestor do
  before(:each) {
    LogAggregator.redis.flushdb
  }

  let(:cql_sample_lines) {
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

  let(:worker_bm_sample_lines) {
    <<-TXT
Dec  9 13:29:35 prod-boglach-worker3 app-boglach[24520]: 13:29:35.297 :85779240 # #WORKER-BM :: {"ts":1418149695,"worker":"ItunesConnect::SyncAppsWorker","queue":"itunes_connect","total":79.59028840065002,"other":12.539522449650036,"cql":0.004121951,"s3":0.0,"sql":67.04664399999999}
Dec  9 13:29:35 prod-boglach-worker17 app-boglach[17510]: 13:29:35.326 :86896300 # #WORKER-BM :: {"ts":1418149775,"worker":"Etl::ItunesConnect::ApiScrapingWorker","queue":"etl_itunes_connect","total":0.039418935775756836,"other":0.007690426775756836,"cql":0.018954768,"s3":0.012773741,"sql":0}
Dec  9 13:29:35 prod-boglach-worker11 app-boglach[15752]: 13:29:35.333 :88201120 # #WORKER-BM :: {"ts":1418149775,"worker":"Etl::ItunesConnect::ApiScrapingWorker","queue":"etl_itunes_connect","total":0.20396065711975098,"other":0.11907253911975096,"cql":0.063488364,"s3":0.021399754,"sql":0}
    TXT
  }

  let(:app_data_processing_sample_lines) {
    <<-TXT
Sep 15 06:49:25 prod-boglach-worker11 app-boglach[1857]: 06:49:25.906 :87102960 # #APP-DATA-PROCESSING :: {"store":"itunes_connect","app_id":547986727,"country_iso":"SG","date":"2015-09-15","data_processing_type":"incremental"}
Sep 15 06:49:25 prod-boglach-worker11 app-boglach[1628]: 06:49:25.921 :88053820 # #APP-DATA-PROCESSING :: {"store":"itunes_connect","app_id":546291130,"country_iso":"MY","date":"2015-09-15","data_processing_type":"incremental"}
Sep 15 06:49:25 prod-boglach-worker306 app-boglach[9751]: 06:49:25.928 :87170080 # #APP-DATA-PROCESSING :: {"store":"itunes_connect","app_id":877420555,"country_iso":"RU","date":"2015-09-15","data_processing_type":"full"}
Sep 15 06:49:25 prod-boglach-worker305 app-boglach[21544]: 06:49:25.936 :97396720 # #APP-DATA-PROCESSING :: {"store":"itunes_connect","app_id":933521425,"country_iso":"TW","date":"2015-09-15","data_processing_type":"incremental"}
Sep 15 06:49:25 prod-boglach-worker305 app-boglach[21543]: 06:59:25.936 :97396720 # #APP-DATA-PROCESSING :: {"store":"itunes_connect","app_id":933521425,"country_iso":"TW","date":"2015-09-15","data_processing_type":"incremental"}
    TXT
  }

  let(:cql_line) {
    'Nov 27 09:50:52 prod-boglach-worker12 app-boglach[8808]: 09:50:52.753 :69180140 # #CQL :: {"type":"query","query":"xyz","ts":1417099952,"t":0.009008195}'
  }

  let(:worker_bm_line) {
    'Nov 27 09:52:35 prod-boglach-worker21 app-boglach[9004]: 09:52:35.159 :92887440 # #WORKER-BM {"ts":1417099951,"worker":"Canonic::GooglePlay::AppBreakoutCalculationsWorker","queue":"google_play_app_breakout_calculations","total":3.3580825328826904,"other":1.4104158228826902,"cql":1.9476667100000002,"s3":0.0,"sql":0}'
  }

  let(:app_data_processing_line) {
    'Sep 15 06:49:26 prod-boglach-worker13 app-boglach[19329]: 06:49:26.198 :86618660 # #APP-DATA-PROCESSING :: {"store":"itunes_connect","app_id":427473420,"country_iso":"PH","date":"2015-09-15","data_processing_type":"incremental"}'
  }

  let(:irrelevant_line) {
    'Nov 27 09:52:35 prod-boglach-worker21 app-boglach[9004]: 09:52:35.159 :92887440 # #BOO {}'
  }

  let(:ingestor) {LogAggregator::Ingestor.new}

  it "handles CQL log lines" do
    cql_sample_lines.each_line {|l|
      ingestor.handle_logline('CQL', l)
    }

    labels, counts, avgs, errors = ingestor.cql_benchmark.minute_series(
      Time.parse('2014-11-27 16:50:00 +0200'),
      Time.parse('2014-11-27 16:52:00 +0200'),
    )

    expect(labels).to eq(["2014-11-27 16:50:00", "2014-11-27 16:51:00", "2014-11-27 16:52:00"])

    expect(counts).to eq({"xyz"=>[2, 0, 2], "abc"=>[2, 0, 1]})

    a1, a2, a3 = avgs["abc"]
    expect(a1).to be_within(0.0001).of(0.0119)
    expect(a2).to be_within(0.0001).of(0.0)
    expect(a3).to be_within(0.0001).of(0.0055)
  end

  it "handles WORKER-BM log lines" do
    worker_bm_sample_lines.each_line {|l|
      ingestor.handle_logline('WORKER-BM', l)
    }

    labels, counts, avgs, errors = ingestor.worker_benchmark.minute_series(
      Time.parse('2014-12-09 20:28:00 +0200'),
      Time.parse('2014-12-09 20:29:00 +0200'),
    )

    expect(labels).to eq(["2014-12-09 20:28:00", "2014-12-09 20:29:00"])
    expect(counts).to eq({"Etl::ItunesConnect::ApiScrapingWorker"=>[0, 2], "ItunesConnect::SyncAppsWorker"=>[1, 0]})
  end

  it "handles APP-DATA-PROCESSING log lines" do
    app_data_processing_sample_lines.each_line {|l|
      ingestor.handle_logline('APP-DATA-PROCESSING', l)
    }

    overall_count = ingestor.app_processing_frequency.get_overall_count('2015-09-15/itunes_connect')
    previously_seen_count = ingestor.app_processing_frequency.get_previously_seen_count('2015-09-15/itunes_connect')
    previously_seen_rate = ingestor.app_processing_frequency.get_previously_seen_rate('2015-09-15/itunes_connect')

    expect(overall_count).to eq(5)
    expect(previously_seen_count).to eq(1)
    expect(previously_seen_rate).to eq(0.2)
  end

  it "recognizes CQL tagged lines" do
    expect(ingestor).to receive(:handle_logline).with('CQL', cql_line)
    ingestor.handle_input_line(cql_line)
  end

  it "recognizes WORKER-BM tagged lines" do
    expect(ingestor).to receive(:handle_logline).with('WORKER-BM', worker_bm_line)
    ingestor.handle_input_line(worker_bm_line)
  end

  it "recognizes APP-DATA-PROCESSING tagged lines" do
    expect(ingestor).to receive(:handle_logline).with('APP-DATA-PROCESSING', app_data_processing_line)
    ingestor.handle_input_line(app_data_processing_line)
  end

  it "ignores irrelevant lines" do
    expect(ingestor).not_to receive(:handle_logline)
    ingestor.handle_input_line(irrelevant_line)
  end
end
