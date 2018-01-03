package mapreduce.design.patterns.summarisation.inverted.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 11.12.2017.
 */
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        StringBuilder builder = new StringBuilder();
        builder.append(iterator.next());
        while (iterator.hasNext()) {
            builder.append(" ").append(iterator.next());
        }
        context.write(key, new Text(builder.toString()));
    }
}
