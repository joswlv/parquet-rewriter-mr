package com.github.joswlv.parquet.rewirter.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.joswlv.parquet.rewirter.TestHdfsBuilder;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetaInfoConverterTest extends TestHdfsBuilder {

  @Test
  void getMetadataTest() throws IOException {
    String path = MetaInfoConverterTest.class.getClassLoader().getResource("SimpleJob.json")
        .getPath();
    System.out.println(path);
    Configuration metadataConfig = MetaInfoConverter.getMetadataConfig(path);

    assertEquals(metadataConfig.get("keyColName"), "custom_no");
    assertEquals(metadataConfig.get("keyColValueList"), "1,2,3");
    assertEquals(metadataConfig.get("targetColNameList"), "address,tel_no,sel_no");
    assertEquals(metadataConfig.get("inputFilePaths"),
        "/user/hive/warehouse/edw.db/sample,/user/hive/warehouse/edw.db/sample/dt=20191111");
  }

}