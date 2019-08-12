package com.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer<Key> extends Reducer<Key,IntWritable,Key, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Key key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
  }
}
