package com.hadoop.mr;

import com.hadoop.bean.PFlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PReducer extends Reducer<PFlowBean, LongWritable, Text,LongWritable> {

    LongWritable v = new LongWritable();
    Text k = new Text();

    @Override
    protected void reduce(PFlowBean key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long value = 0l;
        for (LongWritable allFlow :
                values) {
            value+=allFlow.get();
        }
        k.set(key.getPhoneNumber());
        v.set(value);
        context.write(k,v);
    }
}
