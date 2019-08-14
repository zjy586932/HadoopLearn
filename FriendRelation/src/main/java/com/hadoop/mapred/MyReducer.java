package com.hadoop.mapred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text,Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer friends = new StringBuffer();
        for (Text value :
                values) {
            friends.append(value);
            friends.append(",");
        }
        String s = friends.toString();
        k.set(key);
        v.set(s.substring(0,s.length()-1));
        context.write(k,v);
    }
}
