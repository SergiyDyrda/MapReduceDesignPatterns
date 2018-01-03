package mapreduce.design.patterns.summarisation.average;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 */
public class AverageTuple implements Writable {
    private long count;
    private float average;

    public AverageTuple() {
    }

    public AverageTuple(long count, float average) {
        this.count = count;
        this.average = average;
    }

    public long getCount() {
        return count;
    }

    public AverageTuple setCount(long count) {
        this.count = count;
        return this;
    }

    public float getAverage() {
        return average;
    }

    public AverageTuple setAverage(float average) {
        this.average = average;
        return this;
    }

    @Override
    public String toString() {
        return "{count=" + count +
                ", average=" + average +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeFloat(average);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setCount(dataInput.readLong());
        setAverage(dataInput.readFloat());
    }
}
