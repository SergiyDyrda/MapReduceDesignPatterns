package mapreduce.design.patterns.filter.top.ten;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 12.12.2017.
 */
public class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private TreeMap<Integer, Text> records = new TreeMap<>(new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return Integer.compare(o2, o1);
        }
    });

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            Text val = iterator.next();
            String row = val.toString().trim();
            defineRow(row, context);
            try {
                String rep = getField("Reputation");
                int reputation = Integer.parseInt(rep);
                records.put(reputation, new Text(val.toString().trim()));
                while (records.size() > 10) {
                    records.pollLastEntry();
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text entry : records.values()) {
            context.write(NullWritable.get(), entry);
        }
    }
}
