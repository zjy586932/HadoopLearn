package com.hadoop.hdfs.tq;

import com.hadoop.tq.TQ;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartioner extends Partitioner<TQ, IntWritable> {
    @Override
    public int getPartition(TQ tq, IntWritable wd, int numPartitions) {
        String s = String.valueOf(tq.getYear());
        return s.hashCode()%numPartitions;
    }
}
