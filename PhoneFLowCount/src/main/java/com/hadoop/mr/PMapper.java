package com.hadoop.mr;

import com.hadoop.bean.PFlowBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class PMapper extends Mapper<LongWritable, Text, PFlowBean, LongWritable> {

    PFlowBean phoneDetail = new PFlowBean();
    LongWritable v = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //7 17706396831 192.168.1.1 1024    2048    200
        String[] values = StringUtils.split(value.toString(), '\t');


        phoneDetail.setId(values[0]);
        phoneDetail.setPhoneNumber(values[1]);
        phoneDetail.setIp(values[2]);
        phoneDetail.setUpFlow(Long.valueOf(values[3]));
        phoneDetail.setDownFlow(Long.valueOf(values[4]));
        phoneDetail.setAllFlow(Long.valueOf(values[3])+Long.valueOf(values[4]));

        v.set(phoneDetail.getAllFlow());

        context.write(phoneDetail,v);

    }
}
