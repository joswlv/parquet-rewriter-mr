package com.github.joswlv.parquet.rewirter.util;

import com.github.joswlv.parquet.rewirter.meta.Metadata;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;

public class Config2Metadata {

  private Configuration config;

  public Config2Metadata(Configuration config) {
    this.config = config;
  }

  public Metadata getMetadata() {
    String keyColName = config.get("keyColName");
    Set<String> keyColValueList = new HashSet<>(
        Arrays.asList(config.get("keyColValueList").split(",",-1)));
    Set<String> targetColNameList = new HashSet<>(
        Arrays.asList(config.get("targetColNameList").split(",",-1)));
    return new Metadata(keyColName, keyColValueList, targetColNameList);
  }
}
