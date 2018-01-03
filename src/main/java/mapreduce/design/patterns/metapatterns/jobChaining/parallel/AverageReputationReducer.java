package mapreduce.design.patterns.metapatterns.jobChaining.parallel;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 21.12.2017.
 */
public class AverageReputationReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        long count = 0;
        for (IntWritable val : values) {
            sum += val.get();
            ++count;
        }
        double average = (double) sum / count;
        context.write(key, new DoubleWritable(average));
    }
}
