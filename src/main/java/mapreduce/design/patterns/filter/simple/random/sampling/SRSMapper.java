package mapreduce.design.patterns.filter.simple.random.sampling;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 12.12.2017.
 */
public class SRSMapper extends Mapper<Object, Text, NullWritable, Text> {

    private Integer filterPercentage;
    private Random random = new Random();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        filterPercentage = context.getConfiguration().getInt("percentage", 30);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (random.nextInt(100) < filterPercentage)
            context.write(NullWritable.get(), value);
    }
}
