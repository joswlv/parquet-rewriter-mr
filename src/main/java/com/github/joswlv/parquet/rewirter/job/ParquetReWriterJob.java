package com.github.joswlv.parquet.rewirter.job;

import com.github.joswlv.parquet.rewirter.mapper.ParquetReWriterMapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class ParquetReWriterJob extends Configured {

  public ParquetReWriterJob(Configuration conf) {
    super(conf);
  }

  public boolean runJob(String jobName)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = getConf();

    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(ParquetReWriterJob.class);

    String inputPath = conf.get("inputPath");
    String mapTaskNum = conf.get("mapTaskNum");

    FileInputFormat.addInputPath(job, new Path(inputPath));

    job.setMapperClass(ParquetReWriterMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setInputFormatClass(NLineInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setNumReduceTasks(0);
    job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", Integer.parseInt(mapTaskNum));

    return job.waitForCompletion(true);
  }
}
