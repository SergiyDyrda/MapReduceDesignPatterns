package mapreduce.design.patterns.summarisation.average;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 */
public class AverageReducer extends Reducer<LongWritable, AverageTuple, LongWritable, AverageTuple> {

    @Override
    protected void reduce(LongWritable key, Iterable<AverageTuple> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        float sum = 0;
        for (AverageTuple tuple : values) {
            sum += tuple.getAverage() * tuple.getCount();
            count += tuple.getCount();
        }
        context.write(key, new AverageTuple(count, (sum / count)));
    }
}
