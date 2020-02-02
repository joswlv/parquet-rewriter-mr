package com.github.joswlv.parquet.rewirter.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowGroupWriter implements Closeable {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    private ParquetFileWriter writer;
    private FSDataInputStream incomingStream;

    public RowGroupWriter(Configuration conf, MessageType schema, String targetParquetFilePath) throws IOException {
        Path sourcePath = new Path(targetParquetFilePath);
        this.writer = new ParquetFileWriter(conf, schema, sourcePath, ParquetFileWriter.Mode.OVERWRITE);
        this.incomingStream = sourcePath.getFileSystem(conf).open(sourcePath);
        this.writer.start();
    }

    public void write(BlockMetaData rowGroup) {
        try {
            writer.appendRowGroup(incomingStream, rowGroup, false);
        } catch (IOException e) {
            log.error("rowGroup write error!, ", e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        writer.end(new HashMap<>());

        if (incomingStream != null) {
            incomingStream.close();
        }
    }
}
