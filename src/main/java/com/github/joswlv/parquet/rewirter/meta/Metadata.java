package com.github.joswlv.parquet.rewirter.meta;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class Metadata implements Serializable {

  private String keyColName;
  private Set<String> keyColValueList;
  private Set<String> targetColNameList;
  private List<String> partitionKeyList;

  private CompressionCodecName rewriteDefaultCompressionCodecName = CompressionCodecName.SNAPPY;
  private int rewriteDefaultBlockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
  private int rewriteDefaultPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
  private int rewriteDefaultDictionaryPageSize = (1024 * 2);
  private WriterVersion rewriteParquetFileWriterVersion = WriterVersion.PARQUET_1_0;

  public Metadata(String keyColName, Set<String> keyColValueList, Set<String> targetColNameList) {
    this.keyColName = keyColName;
    this.keyColValueList = keyColValueList;
    this.targetColNameList = targetColNameList;
  }

  public String getKeyColName() {
    return keyColName;
  }

  public Set<String> getKeyColValueList() {
    return keyColValueList;
  }

  public Set<String> getTargetColNameList() {
    return targetColNameList;
  }

  public List<String> getPartitionKeyList() {
    return partitionKeyList;
  }

  public CompressionCodecName getRewriteDefaultCompressionCodecName() {
    return rewriteDefaultCompressionCodecName;
  }

  public int getRewriteDefaultBlockSize() {
    return rewriteDefaultBlockSize;
  }

  public int getRewriteDefaultPageSize() {
    return rewriteDefaultPageSize;
  }

  public int getRewriteDefaultDictionaryPageSize() {
    return rewriteDefaultDictionaryPageSize;
  }

  public WriterVersion getRewriteParquetFileWriterVersion() {
    return rewriteParquetFileWriterVersion;
  }

  @Override
  public String toString() {
    return "Metadata{" +
        ", keyColName='" + keyColName + '\'' +
        ", keyColValueList=" + keyColValueList +
        ", targetColNameList=" + targetColNameList +
        ", rewriteDefaultCompressionCodecName=" + rewriteDefaultCompressionCodecName +
        ", rewriteDefaultBlockSize=" + rewriteDefaultBlockSize +
        ", rewriteDefaultPageSize=" + rewriteDefaultPageSize +
        ", rewriteDefaultDictionaryPageSize=" + rewriteDefaultDictionaryPageSize +
        ", rewriteParquetFileWriterVersion=" + rewriteParquetFileWriterVersion +
        '}';
  }
}
