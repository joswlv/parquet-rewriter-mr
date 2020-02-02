package com.github.joswlv.parquet.rewirter.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncomingReader implements Closeable {

  private Logger log = LoggerFactory.getLogger(this.getClass());
  private ParquetFileReader reader;

  private List<BlockMetaData> rowGroupMetaDataList;
  private List<Statistics<String>> incomingStats;

  private ParquetBlock parquetBlock;
  private int nextBlockIndex = 0;

  public IncomingReader(Configuration conf, String sourceFile, String keyColName)
      throws IOException {
    this.reader = ParquetFileReader.open(conf, new Path(sourceFile));
    rowGroupMetaDataList = this.reader.getRowGroups();

    ColumnPath keyPath = ColumnPath.get(keyColName);

    rowGroupMetaDataList.stream().forEach(b -> {
      Optional<ColumnChunkMetaData> match = b.getColumns().stream()
          .filter(c -> c.getPath().equals(keyPath)).findFirst();
      if (match.isPresent() && match.get().getStatistics() != null) {
        incomingStats.add(match.get().getStatistics());
      } else {
        throw new RuntimeException(
            "Key Column Stats not found for block:" + b + " keyPath:" + keyPath);
      }
    });
  }

  public Stream<ParquetBlock> getData() {
    return StreamSupport
        .stream(new Spliterators.AbstractSpliterator<ParquetBlock>(Long.MAX_VALUE,
            Spliterator.ORDERED) {

          @Override
          public boolean tryAdvance(Consumer<? super ParquetBlock> action) {
            if (hasNextData()) {
              action.accept(parquetBlock);
              return true;
            } else {
              return false;
            }
          }
        }, false);
  }

  private boolean hasNextData() {
    try {
      PageReadStore pageReadStore = reader.readNextRowGroup();
      if (pageReadStore != null) {
        parquetBlock = new ParquetBlock(rowGroupMetaDataList.get(nextBlockIndex), pageReadStore, incomingStats.get(nextBlockIndex));
        nextBlockIndex++;
        return true;
      }

    } catch (IOException e) {
      log.error("record read error!, ", e.getMessage(), e);
    }
    return false;
  }

  @Override
  public void close() throws IOException {

  }
}
