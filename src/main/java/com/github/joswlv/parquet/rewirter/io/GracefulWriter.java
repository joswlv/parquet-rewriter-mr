package com.github.joswlv.parquet.rewirter.io;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GracefulWriter implements Closeable {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    private ParquetWriter<Group> writer;

    public GracefulWriter(Configuration conf, MessageType schema, String targetParquetFilePath) throws IOException {

        Path filePath = new Path(targetParquetFilePath + "_tmp");

        GroupWriteSupport.setSchema(schema, conf);
        GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
        writer = new ParquetWriter<>(filePath,
                groupWriteSupport,
                CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                (1024 * 2),
                false,
                false,
                ParquetProperties.WriterVersion.PARQUET_1_0,
                conf);
    }

    public void write(Group record) {
        try {
            writer.write(record);
        } catch (IOException e) {
            log.error("record write error!, ", e.getMessage(), e);
        }
    }

    public void close() throws IOException {
        writer.close();
    }
}
