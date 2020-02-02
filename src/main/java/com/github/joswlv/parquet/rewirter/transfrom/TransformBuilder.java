package com.github.joswlv.parquet.rewirter.transfrom;


import com.github.joswlv.parquet.rewirter.meta.Metadata;
import org.apache.parquet.schema.MessageType;

public class TransformBuilder {

  public static Transform build(TransformType type, Metadata metadata, MessageType schema) {
    Transform transform;
    switch (type) {
      case Value2Null:
        transform = new Value2Null(metadata, schema);
        break;
      case SkipAbleValue2Null:
        transform = new SkipAbleValue2Null(metadata, schema);
        break;
      default:
        throw new UnsupportedOperationException(type + " is not support.");
    }
    return transform;
  }
}
