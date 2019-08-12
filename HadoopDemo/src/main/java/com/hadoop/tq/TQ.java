package com.hadoop.tq;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TQ implements WritableComparable<TQ> {
    private int year;
    private int month;
    private int day;
    private int temper;


    @Override
    public int compareTo(TQ that) {
        int y = Integer.compare(this.year, that.getYear());
        if(y==0){
            if(this.month==that.getMonth()){
                if(this.day ==that.getDay()){
                    return -Integer.compare(this.temper,that.getTemper());
                }
                return Integer.compare(this.day,that.getDay());
            }
            return Integer.compare(this.month,that.getMonth());
        }
        return y;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.month);
        out.writeInt(this.day);
        out.writeInt(this.temper);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.temper = in.readInt();
    }

    @Override
    public String toString() {
        return this.getYear()+"-"+this.getMonth()+"-"+this.getDay()+":";
    }
}
