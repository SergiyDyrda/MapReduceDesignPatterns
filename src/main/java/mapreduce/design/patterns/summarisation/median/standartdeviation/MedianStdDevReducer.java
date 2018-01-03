package mapreduce.design.patterns.summarisation.median.standartdeviation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 */
public class MedianStdDevReducer extends Reducer<IntWritable, LongWritable, IntWritable, MedianStdDevTuple> {

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long numberOfComments = 0;
        long totalLength = 0;
        List<Long> commentsLengths = new ArrayList<>();

        for (LongWritable length: values) {
            numberOfComments++;
            totalLength += length.get();
            commentsLengths.add(length.get());
        }

        //Median
        Collections.sort(commentsLengths);
        int size = commentsLengths.size();
        int medianIndex = size / 2;
        float median;
        if (size % 2 == 0) {
            median = (commentsLengths.get(medianIndex) + commentsLengths.get(medianIndex + 1)) / 2f;
        } else {
            median = commentsLengths.get(medianIndex);
        }

        //StdDev
        float average = totalLength / numberOfComments * 1f;
        long sumOfSquares = 0;
        for (Long length: commentsLengths) {
            sumOfSquares += (long) Math.pow((length - average), 2);
        }
        float stdDev = (float) Math.sqrt(sumOfSquares / (numberOfComments - 1));

        context.write(key, new MedianStdDevTuple(median, stdDev));
    }
}
