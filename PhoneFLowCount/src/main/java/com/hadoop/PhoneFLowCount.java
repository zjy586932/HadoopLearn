package com.hadoop;

import com.hadoop.bean.PFlowBean;
import com.hadoop.comparator.PGroupComparator;
import com.hadoop.mr.PMapper;
import com.hadoop.mr.PReducer;
import com.hadoop.partitionor.PPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhoneFLowCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration con = new Configuration(true);
        Job job = Job.getInstance(con);

        //map
        job.setJarByClass(PhoneFLowCount.class);
        job.setMapperClass(PMapper.class);
        //map的k，v输出类型
        job.setMapOutputKeyClass(PFlowBean.class);
        job.setMapOutputValueClass(LongWritable.class);
//        job.setSortComparatorClass(PSortComparator.class);
        //分组比较器
        job.setGroupingComparatorClass(PGroupComparator.class);
        //分区partitionor
        job.setPartitionerClass(PPartitioner.class);
        job.setNumReduceTasks(5);

        Path inpath = new Path("/phone/input/phonePartition.txt");
        Path outpath = new Path("/phone/output");

        //输入输出路径
        FileInputFormat.addInputPath(job,inpath);
        if(outpath.getFileSystem(con).exists(outpath)){
            outpath.getFileSystem(con).delete(outpath,true);
        }
        FileOutputFormat.setOutputPath(job,outpath);

        //reduce
        job.setReducerClass(PReducer.class);

        job.waitForCompletion(true);
    }
}
