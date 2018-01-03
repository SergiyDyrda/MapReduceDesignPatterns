package mapreduce.design.patterns.join.cartesianProduct;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 19.12.2017.
 */
public class CartesianRecordReader extends RecordReader<Text, Text> {

    private CompositeInputSplit compositeInputSplit;
    private RecordReader<? extends WritableComparable, ? extends Text> leftRecordReader, rightRecordReader;
    private FileInputFormat<? extends WritableComparable, ? extends Text> leftInputFormat, rightInputFormat;
    private TaskAttemptContext context;

    private Text key, value;

    private boolean done;

    public CartesianRecordReader(FileInputFormat<? extends WritableComparable, ? extends Text> leftInputFormat,
                                 FileInputFormat<? extends WritableComparable, ? extends Text> rightInputFormat) {
        this.leftInputFormat = leftInputFormat;
        this.rightInputFormat = rightInputFormat;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.compositeInputSplit = (CompositeInputSplit) split;
        this.context = context;
        leftRecordReader = initializeRecordReader(leftInputFormat, 0);
        rightRecordReader = initializeRecordReader(rightInputFormat, 1);
    }

    private RecordReader<? extends WritableComparable, ? extends Text> initializeRecordReader(FileInputFormat<? extends WritableComparable, ? extends Text> inputFormat, int splitPosition) throws IOException, InterruptedException {
        RecordReader<? extends WritableComparable, ? extends Text> recordReader = inputFormat.createRecordReader(compositeInputSplit.get(splitPosition), context);
        recordReader.initialize(compositeInputSplit.get(splitPosition), context);
        return recordReader;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            if (leftRecordReader.nextKeyValue()) {
                key = leftRecordReader.getCurrentValue();
            } else {
                done = true;
                return false;
            }
        }
        boolean goToNextLeft;
        if (!rightRecordReader.nextKeyValue()) {
            rightRecordReader = initializeRecordReader(rightInputFormat, 1);
            rightRecordReader.nextKeyValue();
            goToNextLeft = true;
        } else {
            goToNextLeft = false;
        }
        value = rightRecordReader.getCurrentValue();
        if (goToNextLeft) {
            if (leftRecordReader.nextKeyValue()) {
                key = leftRecordReader.getCurrentValue();
            } else {
                done = true;
                return false;
            }
        }
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return !done ? 0.0f : 1.0f;
    }

    @Override
    public void close() throws IOException {
        leftRecordReader.close();
        rightRecordReader.close();
    }
}
