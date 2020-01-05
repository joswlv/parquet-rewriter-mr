package com.github.joswlv.parquet.rewirter.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaInfoConverter {

  private static Logger log = LoggerFactory.getLogger(MetaInfoConverter.class);

  private static ObjectMapper objectMapper = new ObjectMapper();

  private final static String BIGDATA_HDFS_PREFIX = "/user/hive/warehouse/";

  public static Configuration getMetadataConfig(String path) throws IOException {
    JsonNode node = null;

    try {
      node = objectMapper.readTree(Paths.get(path).toFile());
    } catch (IOException e) {
      log.error("Not find JsonFile. {}", e.getMessage());
    }

    String tableFullName = node.get("tableFullName").toString();
    String keyColName = node.get("keyColName").toString();
    String configDirPath = node.get("configDirPath").toString();
    Set<String> keyColValueList = objectMapper
        .convertValue(node.get("keyColValueList"), new TypeReference<HashSet<String>>() {
        });
    Set<String> targetColNameList = objectMapper
        .convertValue(node.get("targetColNameList"), new TypeReference<HashSet<String>>() {
        });

    Configuration config = getConfig(configDirPath);
    DistributedFileSystem dfs = (DistributedFileSystem) DistributedFileSystem.newInstance(config);
    List<String> allInputFilePath = getAllFilePath(new Path(getJobTablePath(tableFullName)), dfs);

    return setMetaConfig(config, keyColName, keyColValueList, targetColNameList, allInputFilePath);
  }

  private static Configuration setMetaConfig(Configuration config, String keyColName,
      Set<String> keyColValueList, Set<String> targetColNameList, List<String> allInputFilePath) {
    config.set("keyColName", keyColName);
    config.set("keyColValueList", keyColValueList.stream().collect(Collectors.joining(",")));
    config.set("targetColNameList", targetColNameList.stream().collect(Collectors.joining(",")));
    config.set("allInputFilePath", allInputFilePath.stream().collect(Collectors.joining(",")));

    return config;
  }

  private static String getJobTablePath(String tableFullName) {
    String[] sp = tableFullName.toLowerCase().split("\\.");
    String DB = sp[0];
    String TABLE = sp[1];

    return BIGDATA_HDFS_PREFIX + DB + ".db/" + TABLE;
  }

  private static List<String> getAllFilePath(Path filePath, FileSystem fs) throws IOException {
    List<String> fileList = new ArrayList<>();
    FileStatus[] fileStatus = fs.listStatus(filePath);
    for (FileStatus fileStat : fileStatus) {
      if (fileStat.isDirectory()) {
        fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
      } else {
        fileList.add(fileStat.getPath().toString());
      }
    }
    return fileList;
  }

  //TODO READ HadoopCluster config
  private static Configuration getConfig(String configDirPath) {
    Configuration conf = new Configuration(false);

    return conf;
  }

}
