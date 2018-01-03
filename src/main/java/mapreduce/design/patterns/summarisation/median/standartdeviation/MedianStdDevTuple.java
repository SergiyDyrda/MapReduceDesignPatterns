package mapreduce.design.patterns.summarisation.median.standartdeviation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 */
public class MedianStdDevTuple implements Writable {
    private float median;
    private float stdDev;

    public MedianStdDevTuple() {
    }

    public MedianStdDevTuple(float median, float stdDev) {
        this.median = median;
        this.stdDev = stdDev;
    }

    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }


    public float getStdDev() {
        return stdDev;
    }

    public void setStdDev(float stdDev) {
        this.stdDev = stdDev;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(median);
        dataOutput.writeFloat(stdDev);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        median = dataInput.readFloat();
        stdDev = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "{median=" + median +
                ", stdDev=" + stdDev +
                '}';
    }
}
