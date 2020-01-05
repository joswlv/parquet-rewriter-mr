package com.github.joswlv.parquet.rewirter.runner;


import com.github.joswlv.parquet.rewirter.job.ParquetReWriterJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class RunJob extends Configured implements Tool {

  public RunJob(Configuration metadataConfig) {
    super(metadataConfig);
  }

  @Override
  public int run(String[] args) throws Exception {
    ParquetReWriterJob parquetReWriterJob = new ParquetReWriterJob(getConf());

    if (parquetReWriterJob.runJob("ParquetReWriter")) {
      return RunnerCode.SUCCESS.code();
    }
    return RunnerCode.FAIL.code();
  }
}
