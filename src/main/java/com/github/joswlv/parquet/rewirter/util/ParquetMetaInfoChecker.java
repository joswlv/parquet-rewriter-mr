package com.github.joswlv.parquet.rewirter.util;

import com.github.joswlv.parquet.rewirter.meta.Metadata;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetMetaInfoChecker {

  private Logger log = LoggerFactory.getLogger(this.getClass());
  private Metadata parquetMetaInfo;
  private Configuration conf;

  public ParquetMetaInfoChecker(Metadata parquetMetaInfo, Configuration conf) {
    this.parquetMetaInfo = parquetMetaInfo;
    this.conf = conf;
  }

  public boolean isProcessing(String parquetFileString) throws IOException {

    Path parquetFile = new Path(parquetFileString);

    ParquetFileReader fileReader = ParquetFileReader.open(conf, parquetFile);

    FileMetaData fileMetaData = fileReader.getFileMetaData();
    parquetMetaInfo.setFileMetaData(fileMetaData);
    parquetMetaInfo.setConf(conf);

    int keyColIndex = fileMetaData.getSchema().getFieldIndex(parquetMetaInfo.getKeyColName());
    Set<String> keyColValueList = parquetMetaInfo.getKeyColValueList();

    List<BlockMetaData> rowGroupsList = fileReader.getRowGroups();
    for (BlockMetaData blockMetaData : rowGroupsList) {

      if (isNotExistStatisticInfo(blockMetaData, keyColIndex)) {
        return true;
      }

      if (isExistColValueInRowGroup(blockMetaData, keyColIndex, keyColValueList)) {
        return true;
      }
    }
    return false;

  }

  private boolean isExistStatisticInfo(BlockMetaData blockMetaData, int keyColIndex) {
    return blockMetaData.getColumns().get(keyColIndex).getStatistics().genericGetMax() != null;
  }

  private boolean isNotExistStatisticInfo(BlockMetaData blockMetaData, int keyColIndex) {
    return !isExistStatisticInfo(blockMetaData, keyColIndex);
  }

  public static boolean isExistColValueInRowGroup(BlockMetaData blockMetaData, int keyColIndex,
      Set<String> keyColValueList) {

    ColumnChunkMetaData columnChunkMetaData = blockMetaData.getColumns().get(keyColIndex);
    for (String keyColValue : keyColValueList) {

      int compareMinResult = compareStatistics(columnChunkMetaData, keyColValue,
          new MinStatisticsCompare());
      int compareMaxResult = compareStatistics(columnChunkMetaData, keyColValue,
          new MaxStatisticsCompare());

      if (compareMinResult <= 0 && compareMaxResult >= 0) {
        return true;
      }
    }

    return false;

  }

  private static int compareStatistics(ColumnChunkMetaData columnChunkMetaData, String keyColValue,
      StatisticsCompare statisticsCompare) {

    PrimitiveTypeName primitiveTypeName = columnChunkMetaData.getPrimitiveType()
        .getPrimitiveTypeName();

    Statistics statistics = columnChunkMetaData.getStatistics();
    switch (primitiveTypeName) {
      case BINARY:
      case INT96:
        return statisticsCompare.compare(statistics, Binary.fromString(keyColValue));
      case DOUBLE:
        return statisticsCompare.compare(statistics, Double.parseDouble(keyColValue));
      case FLOAT:
        return statisticsCompare.compare(statistics, Float.parseFloat(keyColValue));
      case INT32:
        return statisticsCompare.compare(statistics, Integer.parseInt(keyColValue));
      case INT64:
        return statisticsCompare.compare(statistics, Long.parseLong(keyColValue));
      default:
        throw new IllegalArgumentException("Not Support Type!");
    }
  }
}



