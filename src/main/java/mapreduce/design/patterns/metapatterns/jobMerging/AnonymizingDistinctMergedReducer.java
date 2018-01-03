package mapreduce.design.patterns.metapatterns.jobMerging;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 22.12.2017.
 */

public class AnonymizingDistinctMergedReducer extends Reducer<TaggedText, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> mos = null;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(TaggedText key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.getTag().equals("anon")) {
            anonymizeReduce(key.getText(), values, context);
        } else {
            distinctReduce(key.getText(), values, context);
        }
    }
    private void anonymizeReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            mos.write(Driver.MULTIPLE_OUTPUTS_ANONYMIZE, value,
                    NullWritable.get(), Driver.MULTIPLE_OUTPUTS_ANONYMIZE + "/part");
        }
    }
    private void distinctReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        mos.write(Driver.MULTIPLE_OUTPUTS_DISTINCT, key, NullWritable.get(),
                Driver.MULTIPLE_OUTPUTS_DISTINCT + "/part");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
