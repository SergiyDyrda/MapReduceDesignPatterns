package mapreduce.design.patterns.join.cartesianProduct;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 19.12.2017.
 */
public class CartesianInputFormat extends FileInputFormat<Text, Text> {

    private static FileInputFormat<? extends WritableComparable, ? extends Text> leftInputFormat, rightInputFormat;
    private static Path leftInputPath, rightInputPath;

    public static void setLeftInputInfo(Job job, Class<? extends FileInputFormat<? extends WritableComparable, ? extends Text>> inputFormat, Path inputPath) throws IOException {
        leftInputFormat = ReflectionUtils.newInstance(inputFormat, job.getConfiguration());
        leftInputPath = inputPath;
    }

    public static void setRightInputInfo(Job job, Class<? extends FileInputFormat<? extends WritableComparable, ? extends Text>> inputFormat, Path inputPath) throws IOException {
        rightInputFormat = ReflectionUtils.newInstance(inputFormat, job.getConfiguration());
        rightInputPath = inputPath;
    }


    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> leftInputSplits = getInputSplits(leftInputFormat, job, leftInputPath);
        List<InputSplit> rightInputSplits = getInputSplits(rightInputFormat, job, rightInputPath);

        List<InputSplit> resultList = new ArrayList<>(leftInputSplits.size() * rightInputSplits.size());
        for (InputSplit leftInputSplit : leftInputSplits) {
            for (InputSplit rightInputSplit : rightInputSplits) {
                resultList.add(constructCompositeInputSplit(leftInputSplit, rightInputSplit));
            }
        }
        return resultList;
    }

    private CompositeInputSplit constructCompositeInputSplit(InputSplit leftInputSplit, InputSplit rightInputSplit) throws IOException {
        CompositeInputSplit compositeInputSplit = new CompositeInputSplit(2);
        try {
            compositeInputSplit.add(leftInputSplit);
            compositeInputSplit.add(rightInputSplit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return compositeInputSplit;
    }

    private List<InputSplit> getInputSplits(FileInputFormat<? extends WritableComparable, ? extends Text> inputFormat, JobContext job, Path... inputPaths) throws IOException {
        setInputPaths((Job) job, inputPaths);
        return inputFormat.getSplits(job);
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new CartesianRecordReader(leftInputFormat, rightInputFormat);
    }
}
