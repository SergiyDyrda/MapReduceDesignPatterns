package mapreduce.design.patterns.dataOrganization.partitioning.usersByLastAccesDate;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */

public class LastAccessDateReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(val, NullWritable.get());
        }
    }
}
