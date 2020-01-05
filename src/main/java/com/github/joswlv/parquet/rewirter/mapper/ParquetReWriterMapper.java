package com.github.joswlv.parquet.rewirter.mapper;

import com.github.joswlv.parquet.rewirter.meta.Metadata;
import com.github.joswlv.parquet.rewirter.transfrom.Transform;
import com.github.joswlv.parquet.rewirter.transfrom.TransformBuilder;
import com.github.joswlv.parquet.rewirter.transfrom.TransformType;
import com.github.joswlv.parquet.rewirter.util.Config2Metadata;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

public class ParquetReWriterMapper extends Mapper<LongWritable, Group, Void, Group> {

  private Metadata metadata;
  private Transform transform;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();
    Config2Metadata config2Metadata = new Config2Metadata(config);
    metadata = config2Metadata.getMetadata();
    MessageType schema = GroupWriteSupport.getSchema(config);
    transform = TransformBuilder.build(TransformType.Value2Null, metadata, schema);
  }

  @Override
  protected void map(LongWritable key, Group value, Context context)
      throws IOException, InterruptedException {
    context.write(null, (Group) transform.transform(value));
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
  }
}

