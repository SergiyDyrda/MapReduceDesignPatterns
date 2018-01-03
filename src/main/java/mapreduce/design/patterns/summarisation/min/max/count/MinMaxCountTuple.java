package mapreduce.design.patterns.summarisation.min.max.count;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 */
public class MinMaxCountTuple implements Writable {
    private Date first;
    private Date last;
    private long count;

    public MinMaxCountTuple(Date firstDate, Date lastDate, long count) {
        setFirst(firstDate);
        setLast(lastDate);
        setCount(count);
    }

    public MinMaxCountTuple() {
    }

    public Date getFirstDate() {
        return first;
    }

    public MinMaxCountTuple setFirst(Date first) {
        this.first = first;
        return this;
    }

    public Date getLastDate() {
        return last;
    }

    public MinMaxCountTuple setLast(Date last) {
        this.last = last;
        return this;
    }

    public long getCount() {
        return count;
    }

    public MinMaxCountTuple setCount(long count) {
        this.count = count;
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(getFirstDate().getTime());
        out.writeLong(getLastDate().getTime());
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setFirst(new Date(in.readLong()));
        setLast(new Date(in.readLong()));
        setCount(in.readLong());
    }

    @Override
    public String toString() {
        return "{firstDatePost=" + first +
                ", lastDatePost=" + last +
                ", count=" + count +
                '}';
    }
}
