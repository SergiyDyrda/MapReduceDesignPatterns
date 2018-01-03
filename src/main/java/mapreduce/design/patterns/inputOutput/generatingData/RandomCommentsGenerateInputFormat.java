package mapreduce.design.patterns.inputOutput.generatingData;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 26.12.2017.
 */
public class RandomCommentsGenerateInputFormat extends InputFormat<Text, NullWritable> {

    public static final String NUM_RECORDS_PER_TASK = "number.records.per.map.task";
    public static final String NUM_MAP_TASKS = "number.map.input.splits";

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        int size = context.getConfiguration().getInt(NUM_MAP_TASKS, 1);
        List<InputSplit> splits = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            splits.add(new FakeInputSplit());
        }
        return splits;
    }

    @Override
    public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        GenerateCommentsRecordReader recordReader = new GenerateCommentsRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }

    public static void setNumRecordsPerTask(Job job, long numRecordsPerTask) {
        job.getConfiguration().setLong(NUM_RECORDS_PER_TASK, numRecordsPerTask);
    }

    public static void setNumMapTasks(Job job, int numMapTasks) {
        job.getConfiguration().setInt(NUM_MAP_TASKS, numMapTasks);
    }
}
