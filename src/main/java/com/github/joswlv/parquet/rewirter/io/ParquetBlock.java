package com.github.joswlv.parquet.rewirter.io;

import java.io.Serializable;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

public class ParquetBlock implements Serializable {

  private BlockMetaData blockMetaData;
  private PageReadStore pageReadStore;

  public ParquetBlock(BlockMetaData blockMetaData, PageReadStore pageReadStore) {
    this.blockMetaData = blockMetaData;
    this.pageReadStore = pageReadStore;
  }

  public BlockMetaData getBlockMetaData() {
    return blockMetaData;
  }

  public PageReadStore getPageReadStore() {
    return pageReadStore;
  }
}
