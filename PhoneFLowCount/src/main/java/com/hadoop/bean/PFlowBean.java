package com.hadoop.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PFlowBean implements WritableComparable<PFlowBean> {

    private String id;
    private String phoneNumber;
    private String ip;
    private long upFlow;
    private long downFlow;
    private long allFlow;

    @Override
    public int compareTo(PFlowBean that) {
        return this.phoneNumber.compareTo(that.getPhoneNumber());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeUTF(this.phoneNumber);
        out.writeUTF(this.ip);
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.allFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.phoneNumber = in.readUTF();
        this.ip = in.readUTF();
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.allFlow = in.readLong();
    }
}
