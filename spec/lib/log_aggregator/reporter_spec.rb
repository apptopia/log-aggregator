require 'spec_helper'

describe LogAggregator::Reporter do
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

  let(:ingestor) {LogAggregator::Ingestor.new}
  let(:reporter) {LogAggregator::Reporter.new}

  it "prints CQL longest report without errors" do
    cql_sample_lines.each_line {|l|
      ingestor.handle_logline('CQL', l)
    }

    l = lambda {
      reporter.cql_print_longest(
        Time.parse('2014-11-27 16:50:00'),
        Time.parse('2014-11-27 16:52:00'),
        10
      )
    }

    expect(l).to_not raise_error
  end

  it "prints CQL hottest report without errors" do
    cql_sample_lines.each_line {|l|
      ingestor.handle_logline('CQL', l)
    }

    l = lambda {
      reporter.cql_print_hottest(
        Time.parse('2014-11-27 16:50:00'),
        Time.parse('2014-11-27 16:52:00'),
        10
      )
    }

    expect(l).to_not raise_error
  end

  it "prints WORKER-BM longest report without errors" do
    worker_bm_sample_lines.each_line {|l|
      ingestor.handle_logline('WORKER-BM', l)
    }

    l = lambda {
      reporter.worker_print_longest(
        Time.parse('2014-12-09 20:28:00'),
        Time.parse('2014-12-09 20:29:00'),
        10
      )
    }

    expect(l).to_not raise_error
  end

  it "prints WORKER-BM hottest report without errors" do
    worker_bm_sample_lines.each_line {|l|
      ingestor.handle_logline('WORKER-BM', l)
    }

    l = lambda {
      reporter.worker_print_hottest(
        Time.parse('2014-12-09 20:28:00'),
        Time.parse('2014-12-09 20:29:00'),
        10
      )
    }

    expect(l).to_not raise_error
  end
end
