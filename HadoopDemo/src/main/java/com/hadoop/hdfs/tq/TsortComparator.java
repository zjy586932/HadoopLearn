package com.hadoop.hdfs.tq;


import com.hadoop.tq.TQ;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TsortComparator extends WritableComparator {

    public TsortComparator(){
        super(TQ.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TQ t1 = (TQ) a;
        TQ that = (TQ) b;
        int y = Integer.compare(t1.getYear(), that.getYear());
        if(y==0){
            if(t1.getMonth()==that.getMonth()){
                if(t1.getDay() ==that.getDay()){
                    return -Integer.compare(t1.getTemper(),that.getTemper());
                }
                return Integer.compare(t1.getDay(),that.getDay());
            }
            return Integer.compare(t1.getMonth(),that.getMonth());
        }
        return y;
    }
}
