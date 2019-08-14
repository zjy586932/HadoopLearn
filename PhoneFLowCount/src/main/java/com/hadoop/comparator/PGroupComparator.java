package com.hadoop.comparator;

import com.hadoop.bean.PFlowBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PGroupComparator extends WritableComparator {

    /**
     *     此处需要构造函数，引用父类构造函数指定比较器的默认序列化对象
     *     √super(PFlowBean.class,true);
     *     ×super(PFlowBean.class);
     */
    public PGroupComparator(){
        super(PFlowBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PFlowBean pf1 = (PFlowBean) a;
        PFlowBean pf2 = (PFlowBean) b;
        return pf1.getPhoneNumber().compareTo(pf2.getPhoneNumber());
    }
}
