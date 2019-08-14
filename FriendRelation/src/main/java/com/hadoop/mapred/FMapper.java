package com.hadoop.mapred;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FMapper extends Mapper<LongWritable, Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //        A:I,K,C,B,G,F,H,O,D
        String[] in = StringUtils.split(value.toString(), ':');
        String in0 = in[0];
        String[] in1 = StringUtils.split(in[1], ',');

        for (int i = 0; i < in1.length; i++) {
            k.set(in1[i]);
            v.set(in0);
            //I A
            //K A
            context.write(k,v);
        }
    }

}
