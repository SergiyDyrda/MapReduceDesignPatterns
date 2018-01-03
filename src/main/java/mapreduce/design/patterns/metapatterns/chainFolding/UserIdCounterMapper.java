package mapreduce.design.patterns.metapatterns.chainFolding;

import mapreduce.design.patterns.util.InputRowUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 22.12.2017.
 */
public class UserIdCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private LongWritable ONE = new LongWritable(1);
    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        InputRowUtil.defineRow(row, context);
        try {
            String userId = InputRowUtil.getField("OwnerUserId");
            outKey.set(userId);
            context.write(outKey, ONE);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
        }
    }
}
