package com.hadoop.hdfs;

import com.hadoop.mr.MyMapper;
import com.hadoop.mr.MyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyWordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(true);

        Job job = new Job(conf);

        // Create a new Job
       job.setJarByClass(MyWordCount.class);
       // Specify various job-specific parameters
       job.setJobName("myjob");

        Path path = new Path("/ooxx/test1.txt");
        FileInputFormat.addInputPath(job,path);

        Path out = new Path("/ooxx/output");
        if(out.getFileSystem(conf).exists(out)){
            out.getFileSystem(conf).delete(out,true);
        }

        FileOutputFormat.setOutputPath(job,out);

//      job.inputpath(new Path("in"));
//      job.setOutputPath(new Path("out"));
      job.setMapperClass(MyMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

      job.setReducerClass(MyReducer.class);

      // Submit the job, then poll for progress until the job is complete
      job.waitForCompletion(true);
    }
}
