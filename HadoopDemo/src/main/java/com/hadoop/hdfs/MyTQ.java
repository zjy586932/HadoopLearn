package com.hadoop.hdfs;

import com.hadoop.hdfs.tq.TPartioner;
import com.hadoop.hdfs.tq.TgroupComparator;
import com.hadoop.hdfs.tq.TsortComparator;
import com.hadoop.tq.TMapper;
import com.hadoop.tq.TQ;
import com.hadoop.tq.TReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyTQ {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration config = new Configuration(true);
        Job job = Job.getInstance(config);

        //map start
        /**
         * job.setJarByClass(MyTQ.class)
         * 本地跑不需要该行代码，yarn集群下无该代码报错ClassNotFoundException,
         */
        job.setJarByClass(MyTQ.class);//集群下执行需要加该行代码
        job.setMapperClass(TMapper.class);
        //map输出格式
        job.setMapOutputKeyClass(TQ.class);
        job.setMapOutputValueClass(IntWritable.class);
        //分区器
        job.setPartitionerClass(TPartioner.class);
        //排序比较器
        job.setSortComparatorClass(TsortComparator.class);
        //分组比较器
        job.setGroupingComparatorClass(TgroupComparator.class);
        //文件读取路径
        Path inpath = new Path("/tq/input");
        FileInputFormat.addInputPath(job,inpath);

        Path outpath = new Path("/tq/output");
        if(outpath.getFileSystem(config).exists(outpath)){
            outpath.getFileSystem(config).delete(outpath,true);
        }
        FileOutputFormat.setOutputPath(job,outpath);
        //map end;


        //reduce start
//        job.setGroupingComparatorClass(TGroupingComparator.class);
        job.setReducerClass(TReducer.class);
        //reduce end;
        job.waitForCompletion(true);
    }
}
