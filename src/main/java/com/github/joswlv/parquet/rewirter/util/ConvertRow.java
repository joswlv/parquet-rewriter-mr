package com.github.joswlv.parquet.rewirter.util;

import java.util.Set;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

public class ConvertRow {
    private String keyColName;
    private Set<String> keyColValueList;
    private Set<String> targetColNameList;
    private MessageType schema;

    public ConvertRow(String keyColName, Set<String> keyColValueList, Set<String> targetColNameList, MessageType schema) {
        this.keyColName = keyColName;
        this.keyColValueList = keyColValueList;
        this.targetColNameList = targetColNameList;
        this.schema = schema;
    }

    public boolean isConvertRow(Group group) {
        int keyColIndex = schema.getFieldIndex(keyColName);
        return keyColValueList.contains(group.getValueToString(keyColIndex, 0));
    }

    public boolean isNotConvertRow(Group group) {
        return !isConvertRow(group);
    }

    public boolean isConvertNullColumn(int fieldIndex) {
        return targetColNameList.contains(schema.getFieldName(fieldIndex));
    }

    public boolean isNotConvertNullColumn(int fieldIndex) {
        return !isConvertNullColumn(fieldIndex);
    }

    public void addColValue(Group newRecord, int index, Group preRecord) {
        GroupType type = preRecord.getType();
        PrimitiveType.PrimitiveTypeName columnType = type.getType(index).asPrimitiveType().getPrimitiveTypeName();
        String fieldName = type.getFieldName(index);

        int fieldRepetitionCount = preRecord.getFieldRepetitionCount(index);
        for (int fieldIndex = 0; fieldIndex < fieldRepetitionCount; fieldIndex++) {
            switch (columnType) {
                case BINARY:
                    newRecord.add(fieldName, preRecord.getBinary(schema.getFieldName(index), fieldIndex));
                    break;
                case BOOLEAN:
                    newRecord.add(fieldName, preRecord.getBoolean(schema.getFieldName(index), fieldIndex));
                    break;
                case DOUBLE:
                    newRecord.add(fieldName, preRecord.getDouble(schema.getFieldName(index), fieldIndex));
                    break;
                case FLOAT:
                    newRecord.add(fieldName, preRecord.getFloat(schema.getFieldName(index), fieldIndex));
                    break;
                case INT32:
                    newRecord.add(fieldName, preRecord.getInteger(schema.getFieldName(index), fieldIndex));
                    break;
                case INT64:
                    newRecord.add(fieldName, preRecord.getLong(schema.getFieldName(index), fieldIndex));
                    break;
                case INT96:
                    newRecord.add(fieldName, preRecord.getInt96(schema.getFieldName(index), fieldIndex));
                    break;
                default:
                    throw new IllegalArgumentException("Not Support Type!");
            }
        }
    }
}
