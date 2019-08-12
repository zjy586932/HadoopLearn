package com.hadoop.hdfs.tq;

import com.hadoop.tq.TQ;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TgroupComparator extends WritableComparator {

    public TgroupComparator(){
        super(TQ.class,true);
    }

    //只比较年月两个维度,相同key为一组进行reduce
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TQ t1 = (TQ) a;
        TQ that = (TQ) b;
        int y = Integer.compare(t1.getYear(), that.getYear());
        if(y==0){
            return Integer.compare(t1.getMonth(),that.getMonth());
        }
        return y;
    }
}
