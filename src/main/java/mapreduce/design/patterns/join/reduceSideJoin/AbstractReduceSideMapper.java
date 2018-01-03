package mapreduce.design.patterns.join.reduceSideJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 18.12.2017.
 */
public abstract class AbstractReduceSideMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected Text outKey = new Text();
    protected Text outValue = new Text();
    protected String tag, field;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String id = getField(field);
            outKey.set(id);
            outValue.set(tag + "\t" + row);
            context.write(outKey, outValue);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
