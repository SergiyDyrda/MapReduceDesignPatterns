package mapreduce.design.patterns.filter.top.ten;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 12.12.2017.
 */
public class TopTenMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private TreeMap<Integer, Text> records = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String rep = getField("Reputation");
            int reputation = Integer.parseInt(rep);
            records.put(reputation, new Text(row));
            while (records.size() > 10) {
                records.pollFirstEntry();
            }
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text entry : records.values()) {
            context.write(NullWritable.get(), entry);
        }
    }
}
