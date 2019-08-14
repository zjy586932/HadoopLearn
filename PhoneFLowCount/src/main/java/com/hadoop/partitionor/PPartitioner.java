package com.hadoop.partitionor;

import com.hadoop.bean.PFlowBean;
import org.apache.hadoop.mapreduce.Partitioner;

public class PPartitioner<V> extends Partitioner<PFlowBean,V> {
    @Override
    public int getPartition(PFlowBean pFlowBean, V value, int numPartitions) {
        String location = pFlowBean.getPhoneNumber().substring(0, 3);
        int partition = 4;
        if("136".equals(location)){
            partition = 0;
        }
        if("137".equals(location)){
            partition = 1;
        }
        if("138".equals(location)){
            partition = 2;
        }
        if("139".equals(location)){
            partition = 3;
        }
        return partition;
    }
}
