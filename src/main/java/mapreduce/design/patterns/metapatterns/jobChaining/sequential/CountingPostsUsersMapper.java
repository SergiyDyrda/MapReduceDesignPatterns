package mapreduce.design.patterns.metapatterns.jobChaining.sequential;

import mapreduce.design.patterns.util.InputRowUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 21.12.2017.
 */
public class CountingPostsUsersMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private LongWritable ONE = new LongWritable(1);
    private Text KEY = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        try {
            InputRowUtil.defineRow(row, context);
            String ownerUserId = InputRowUtil.getField("OwnerUserId");
            KEY.set(ownerUserId);
            context.write(KEY, ONE);
            context.getCounter(Driver.AVERAGE_COUNTER_GROUP, Driver.RECORDS_COUNTER).increment(1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
