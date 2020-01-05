package com.github.joswlv.parquet.rewirter;

import com.github.joswlv.parquet.rewirter.runner.RunJob;
import com.github.joswlv.parquet.rewirter.util.MetaInfoConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver {

  private static Logger log = LoggerFactory.getLogger(Driver.class);

  public static void main(String[] args) {

    String jobPath = args[0];
    try {
      Configuration metadataConfig = MetaInfoConverter.getMetadataConfig(jobPath);
      int res = ToolRunner.run(metadataConfig, new RunJob(metadataConfig), args);
      System.exit(res);
    } catch (Exception e) {
      log.error("Exception", e);
      System.exit(-1);
    }
  }
}
