package mapreduce.design.patterns.metapatterns.jobChaining.sequential;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 21.12.2017.
 */
public class CountingPostsUsersReducer extends LongSumReducer<Text> {

    @Override
    public void reduce(Text text, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(text, values, context);
        context.getCounter(Driver.AVERAGE_COUNTER_GROUP, Driver.USERS_COUNTER).increment(1);
    }
}
