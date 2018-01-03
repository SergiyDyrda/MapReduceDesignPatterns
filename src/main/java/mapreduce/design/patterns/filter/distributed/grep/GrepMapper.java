package mapreduce.design.patterns.filter.distributed.grep;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 12.12.2017.
 */

public class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {
    private String mapRegex = null;

    public void setup(Context context) throws IOException,
            InterruptedException {
        mapRegex = context.getConfiguration().get("mapregex");
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (value.toString().matches(mapRegex)) {
            context.write(NullWritable.get(), value);
        }
    }
}
