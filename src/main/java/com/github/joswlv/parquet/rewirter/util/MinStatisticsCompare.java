package com.github.joswlv.parquet.rewirter.util;

import org.apache.parquet.column.statistics.Statistics;

public class MinStatisticsCompare<U extends Comparable<U>> implements StatisticsCompare<Statistics, U, Integer> {

  @Override
  public int compare(Statistics statisticsCol, U keyColValue) {
    return statisticsCol.compareMinToValue(keyColValue);
  }
}
