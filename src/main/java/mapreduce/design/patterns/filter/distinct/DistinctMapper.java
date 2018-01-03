package mapreduce.design.patterns.filter.distinct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 13.12.2017.
 */

public class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text key = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String userId = getField("UserId", "UserDisplayName");
            this.key.set(userId);
            context.write(this.key, NullWritable.get());
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
}
