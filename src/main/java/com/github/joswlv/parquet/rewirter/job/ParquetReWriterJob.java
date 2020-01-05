package com.github.joswlv.parquet.rewirter.job;

import com.github.joswlv.parquet.rewirter.mapper.ParquetReWriterMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

public class ParquetReWriterJob extends Configured {

  public ParquetReWriterJob(Configuration conf) {
    super(conf);
  }

  public boolean runJob(String jobName)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = getConf();

    //TODO FilerBuilder 만들어야함.
//    ParquetInputFormat.setFilterPredicate(conf, FilterApi.eq(FilterApi.intColumn("line"), -1000));
//    final String fpString = conf.get(ParquetInputFormat.FILTER_PREDICATE);
//    conf.set("parquet.task.side.metadata", "true");
//    conf.set(ParquetInputFormat.FILTER_PREDICATE, fpString);

    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(ParquetReWriterJob.class);
    List<String> inputPaths = Arrays.asList(conf.get("allInputFilePath").split(","));

    for (String inputPath : inputPaths) {
      FileInputFormat.addInputPath(job, new Path(inputPath));
      FileInputFormat.addInputPath(job, new Path(inputPath));
    }
    job.setMapperClass(ParquetReWriterMapper.class);
    job.setMapOutputKeyClass(Void.class);
    job.setMapOutputValueClass(Group.class);

    job.setInputFormatClass(ParquetInputFormat.class);
    job.setOutputFormatClass(ParquetOutputFormat.class);

    ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
    ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);

    job.setNumReduceTasks(0);

    return job.waitForCompletion(true);
  }
}
