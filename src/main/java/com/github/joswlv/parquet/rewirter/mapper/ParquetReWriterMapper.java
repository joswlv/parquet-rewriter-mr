package com.github.joswlv.parquet.rewirter.mapper;

import com.github.joswlv.parquet.rewirter.io.GracefulReader;
import com.github.joswlv.parquet.rewirter.io.GracefulWriter;
import com.github.joswlv.parquet.rewirter.meta.Metadata;
import com.github.joswlv.parquet.rewirter.transfrom.TransformBuilder;
import com.github.joswlv.parquet.rewirter.transfrom.TransformType;
import com.github.joswlv.parquet.rewirter.transfrom.Value2Null;
import com.github.joswlv.parquet.rewirter.util.Config2Metadata;
import com.github.joswlv.parquet.rewirter.util.ParquetMetaInfoChecker;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetReWriterMapper extends Mapper<LongWritable, Text, Text, Text> {

  private static Logger log = LoggerFactory.getLogger(ParquetReWriterMapper.class);

  private Metadata metadata;
  private Value2Null value2Null;
  private Configuration conf;
  private MessageType schema;
  private ParquetMetaInfoChecker parquetMetaInfoChecker;

  private enum Statistics {
    SKIPPING_JOB_FILE, PROCESSING_JOB_FILE
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();
    schema = GroupWriteSupport.getSchema(conf);
    Config2Metadata config2Metadata = new Config2Metadata(conf);
    metadata = config2Metadata.getMetadata();
    value2Null = (Value2Null) TransformBuilder.build(TransformType.Value2Null, metadata, schema);
    parquetMetaInfoChecker = new ParquetMetaInfoChecker(metadata, conf);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    String jobFilePath = value.toString();

    if (parquetMetaInfoChecker.isProcessing(jobFilePath)) {
      try (GracefulReader gracefulReader = new GracefulReader(conf, jobFilePath);
          GracefulWriter gracefulWriter = new GracefulWriter(conf, schema, jobFilePath)) {

        gracefulReader
            .getData()
            .map(value2Null::transform)
            .forEach(gracefulWriter::write);

      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
      context.getCounter(Statistics.PROCESSING_JOB_FILE).increment(1);
    } else {
      context.getCounter(Statistics.SKIPPING_JOB_FILE).increment(1);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
  }
}

