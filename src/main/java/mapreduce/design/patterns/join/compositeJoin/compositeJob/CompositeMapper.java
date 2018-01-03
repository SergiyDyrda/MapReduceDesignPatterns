package mapreduce.design.patterns.join.compositeJoin.compositeJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 18.12.2017.
 */
public class CompositeMapper extends Mapper<Text, TupleWritable, Text, Text> {

    @Override
    protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
        context.write((Text) value.get(0), (Text) value.get(1));
    }
}
