package com.github.joswlv.parquet.rewirter.transfrom;

import com.github.joswlv.parquet.rewirter.io.ParquetBlock;
import com.github.joswlv.parquet.rewirter.meta.Metadata;
import com.github.joswlv.parquet.rewirter.util.ParquetMetaInfoChecker;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class SkipAbleValue2Null implements Transform<ParquetBlock, Void> {
    static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
        Map<K, Set<V>> setMultiMap = new HashMap<>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            Set<V> set = new HashSet<>();
            set.add(entry.getValue());
            setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
        }
        return Collections.unmodifiableMap(setMultiMap);
    }

    private String keyColName;
    private Set<String> keyColValueList;
    private Set<String> targetColNameList;
    private SimpleGroupFactory rowGroupFactory;
    private MessageType schema;
    private GroupReadSupport readSupport;
    private FileMetaData fileMetaData;
    private Configuration conf;

    public SkipAbleValue2Null(Metadata metadata, MessageType schema) {
        this.keyColName = metadata.getKeyColName();
        this.keyColValueList = metadata.getKeyColValueList();
        this.targetColNameList = metadata.getTargetColNameList();
        this.fileMetaData = metadata.getFileMetaData();
        this.conf = metadata.getConf();
        this.schema = schema;
        this.readSupport = new GroupReadSupport();
        this.rowGroupFactory = new SimpleGroupFactory(schema);
    }

    @Override
    public Void transform(ParquetBlock parquetBlock) {
        BlockMetaData blockMetaData = parquetBlock.getBlockMetaData();
        PageReadStore pageReadStore = parquetBlock.getPageReadStore();

        ReadSupport.ReadContext context = readSupport.init(new InitContext(conf, toSetMultiMap(fileMetaData.getKeyValueMetaData()), schema));
        RecordMaterializer<Group> recordConverter = readSupport.prepareForRead(conf, fileMetaData.getKeyValueMetaData(), schema, context);
        ColumnIOFactory ioFactory = new ColumnIOFactory(false);
        MessageColumnIO columnIO = ioFactory.getColumnIO(schema);
        RecordReader<Group> recordReader = columnIO.getRecordReader(pageReadStore, recordConverter);
        int keyColIndex = fileMetaData.getSchema().getFieldIndex(keyColName);

        //TODO 통계정보 활용하는 부분 추상화
        boolean existColValueInRowGroup = ParquetMetaInfoChecker
            .isExistColValueInRowGroup(blockMetaData, keyColIndex, keyColValueList);

        if (existColValueInRowGroup) {

        } else {

        }

        return null;
    }
}
