package mapreduce.design.patterns.metapatterns.chainFolding;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 22.12.2017.
 */
public class UserBinningMapper extends Mapper<Text, LongWritable, Text, LongWritable> {

    private MultipleOutputs<Text, LongWritable> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        if (Long.parseLong(key.toString().trim().split("\t")[1]) < 5000) {
            multipleOutputs.write(Driver.BELOW_OUTPUT_PATH, key, value, Driver.BELOW_OUTPUT_PATH + "/part");
        } else {
            multipleOutputs.write(Driver.ABOVE_OUTPUT_PATH, key, value, Driver.ABOVE_OUTPUT_PATH + "/part");
        }
    }
}
