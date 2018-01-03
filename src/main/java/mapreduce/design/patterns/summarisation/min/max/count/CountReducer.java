package mapreduce.design.patterns.summarisation.min.max.count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Date;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 */

public class CountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
        Date firstDate = new Date();
        Date lastDate = new Date(1);
        long count = 0;
        for (MinMaxCountTuple value : values) {
            if (firstDate.after(value.getFirstDate())) {
                firstDate = value.getFirstDate();
            }
            if (lastDate.before(value.getLastDate())) {
                lastDate = value.getLastDate();
            }
            count += value.getCount();
        }

        context.write(new Text(key.toString()), new MinMaxCountTuple(firstDate, lastDate, count));
    }
}
