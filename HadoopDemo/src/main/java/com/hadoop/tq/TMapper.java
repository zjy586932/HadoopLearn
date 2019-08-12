package com.hadoop.tq;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TMapper extends Mapper<LongWritable, Text, TQ, IntWritable> {

    private TQ tq = new TQ();
    private static IntWritable mval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1949-10-02 12:23:00   34c
        try {
            String[] split = StringUtils.split(value.toString(), '\t');
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date date = null;
            date = format.parse(split[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            tq.setYear(calendar.get(Calendar.YEAR));
            tq.setMonth(calendar.get(Calendar.MONTH)+1);
            tq.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            int wd = Integer.parseInt(split[1].substring(0,split[1].length()-1));
            tq.setTemper(wd);
            mval.set(wd);
            context.write(tq,mval);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }




}
