package com.hadoop.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TReducer extends Reducer<TQ, IntWritable, Text,IntWritable> {

    Text k = new Text();

    //相同key为一组,其中key值来自分组比较器中定义的key
    //TQ{1949-10-02} {34,35,36}
    @Override
    protected void reduce(TQ key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int flag = 0;
        int day = 0;
        for (IntWritable wd :
                values) {
            if (flag == 0){
                k.set(key.toString());
                context.write(k,wd);
                flag++;
                day = key.getDay();
            }
            if(flag!=0 && day == key.getDay()){
                k.set(key.toString());
                context.write(k,wd);
                break;
            }
        }
    }
}
