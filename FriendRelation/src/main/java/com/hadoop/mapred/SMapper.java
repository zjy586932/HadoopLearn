package com.hadoop.mapred;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;

public class SMapper extends Mapper<LongWritable, Text,Text,Text> {
    String[] in ;
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        A	D,O,B,F,C,E
        in = StringUtils.split(value.toString(), '\t');
        String men = in[0];
        String[] friends = StringUtils.split(in[1], ',');
        Arrays.sort(friends);
        for (int i = 0; i < friends.length; i++) {
            for (int j = i+1; j < friends.length; j++) {
                k.set(friends[i]+"-"+friends[j]);
                v.set(men);
                context.write(k,v);
            }
        }
    }
}
