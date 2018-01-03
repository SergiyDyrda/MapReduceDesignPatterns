package mapreduce.design.patterns.join.reduceSideJoin.withBloomFilter;

import mapreduce.design.patterns.join.reduceSideJoin.AbstractReduceSideMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 18.12.2017.
 */
public class UserReduceSideMapperWithBloomFilter extends AbstractReduceSideMapper {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tag = "user";
        field = "Id";
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String id = getField(field);
            String reputation = getField("Reputation");
            if (Integer.parseInt(reputation) < 1500) return;
            outKey.set(id);
            outValue.set(tag + "\t" + row);
            context.write(outKey, outValue);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
