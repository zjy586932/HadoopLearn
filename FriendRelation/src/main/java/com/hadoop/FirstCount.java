package com.hadoop;

import com.hadoop.mapred.FMapper;
import com.hadoop.mapred.MyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FirstCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration con = new Configuration(true);
        Job job = Job.getInstance(con);

        job.setJarByClass(FirstCount.class);

        //map
        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        Path inpath = new Path("/friendship/friendship.txt");
        FileInputFormat.addInputPath(job,inpath);
        Path outpath = new Path("/friendship/output/");
        if(outpath.getFileSystem(con).exists(outpath)){
            outpath.getFileSystem(con).delete(outpath,true);
        }
        FileOutputFormat.setOutputPath(job,outpath);


        //reduce
        job.setReducerClass(MyReducer.class);

        job.waitForCompletion(true);
    }
}
