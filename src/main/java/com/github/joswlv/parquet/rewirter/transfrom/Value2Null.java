package com.github.joswlv.parquet.rewirter.transfrom;


import com.github.joswlv.parquet.rewirter.meta.Metadata;
import com.github.joswlv.parquet.rewirter.util.ConvertRow;
import java.util.Set;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;

public class Value2Null implements Transform<Group, Group> {

  private String keyColName;
  private Set<String> keyColValueList;
  private Set<String> targetColNameList;
  private SimpleGroupFactory rowGroupFactory;
  private MessageType schema;
  private ConvertRow convertRow;

  public Value2Null(Metadata metadata, MessageType schema) {
    this.keyColName = metadata.getKeyColName();
    this.keyColValueList = metadata.getKeyColValueList();
    this.targetColNameList = metadata.getTargetColNameList();
    this.schema = schema;
    this.rowGroupFactory = new SimpleGroupFactory(schema);
    this.convertRow = new ConvertRow(keyColName, keyColValueList, targetColNameList, schema);
  }

  @Override
  public Group transform(Group preRecord) {

    if (convertRow.isNotConvertRow(preRecord)) {
      return preRecord;
    }

    Group newRecord = rowGroupFactory.newGroup();
    int fieldCount = schema.getFieldCount();
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
      if (convertRow.isConvertNullColumn(fieldIndex)) {
        continue;
      }
      convertRow.addColValue(newRecord, fieldIndex, preRecord);
    }

    return newRecord;
  }

}
