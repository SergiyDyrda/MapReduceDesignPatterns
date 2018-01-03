package mapreduce.design.patterns.dataOrganization.totalOrderSorting.userByLastAccessDate;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 15.12.2017.
 */
public class OrderReducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(val, NullWritable.get());
        }
    }
}
