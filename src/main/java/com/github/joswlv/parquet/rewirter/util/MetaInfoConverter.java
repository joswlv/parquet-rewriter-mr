package com.github.joswlv.parquet.rewirter.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class MetaInfoConverter {

  private final static String BIGDATA_HDFS_PREFIX = "/user/hive/warehouse/";

  public static Configuration getMetadataConfig(String path, String inputPath) throws IOException {
    Properties prop = new Properties();
    prop.load(Files.newInputStream(Paths.get(path)));

    String tableFullName = prop.getProperty("tableFullName");
    String mapTaskNum = prop.getProperty("mapTaskNum");
    String keyColName = prop.getProperty("keyColName");
    Set<String> keyColValueList = Arrays.stream(prop.getProperty("keyColValueList").split(",", -1)).collect(Collectors.toSet());
    Set<String> targetColNameList = Arrays.stream(prop.getProperty("targetColNameList").split(",", -1)).collect(Collectors.toSet());

    Configuration config = new Configuration();
    DistributedFileSystem dfs = (DistributedFileSystem) DistributedFileSystem.newInstance(config);
    String inputDirPath = getJobTablePath(tableFullName);
    List<String> allInputFilePath = getAllFilePath(new Path(inputDirPath), dfs);
    String parquetFileSchema = getFileSchema(config, allInputFilePath.get(0));
    return setMetaConfig(config, keyColName, tableFullName, keyColValueList, targetColNameList, inputDirPath, parquetFileSchema, mapTaskNum, inputPath);
  }

  private static Configuration setMetaConfig(Configuration config, String keyColName, String tableFullName,
                                             Set<String> keyColValueList, Set<String> targetColNameList, String inputDirPath, String parquetFileSchema, String mapTaskNum, String inputPath) {
    config.set("keyColName", keyColName.replace("\"", ""));
    config.set("tableFullName", tableFullName.replace("\"", ""));
    config.set("keyColValueList", keyColValueList.stream().collect(Collectors.joining(",")).replace("\"", ""));
    config.set("targetColNameList", targetColNameList.stream().collect(Collectors.joining(",")).replace("\"", ""));
    config.set("inputDirPath", inputDirPath);
    config.set("mapTaskNum", mapTaskNum);
    config.set("parquet.example.schema", parquetFileSchema);
    config.set("inputPath", inputPath);

    return config;
  }

  private static String getJobTablePath(String tableFullName) {
    String[] sp = tableFullName.toLowerCase().split("\\.");
    String DB = sp[0];
    String TABLE = sp[1];

    String dbTableName = DB + ".db/" + TABLE;
    String JobPath = BIGDATA_HDFS_PREFIX + dbTableName.replace("\"","");
    return JobPath.replace("\"","");
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

  private static String getFileSchema(Configuration config, String parquetFileOnePath) {

    try {
      ParquetMetadata parquetMetadata = ParquetFileReader
              .readFooter(config,
                      new Path(parquetFileOnePath),
                      ParquetMetadataConverter.NO_FILTER);
      return parquetMetadata.getFileMetaData().getSchema().toString();
    } catch (Exception e) {
      throw new RuntimeException("File list is empty, maybe incorrect input path or key value column isn't contain in parquet File ");
    }
  }
}
