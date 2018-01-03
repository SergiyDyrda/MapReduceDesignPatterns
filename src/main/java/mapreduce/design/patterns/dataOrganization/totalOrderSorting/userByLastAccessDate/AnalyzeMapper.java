package mapreduce.design.patterns.dataOrganization.totalOrderSorting.userByLastAccessDate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 15.12.2017.
 */
public class AnalyzeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String lastAccessDate = getField("LastAccessDate");
            outKey.set(lastAccessDate);
            context.write(outKey, value);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
