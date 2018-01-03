package mapreduce.design.patterns.metapatterns.jobChaining.parallel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 21.12.2017.
 */
public class AverageReputationMapper extends Mapper<Text, Text, Text, IntWritable> {

    private Text GROUP_KEY = new Text("Average reputation:");
    private IntWritable value = new IntWritable();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String reputationStr = value.toString().trim().split("\t")[0].split(":")[1];
        if (reputationStr.equals("null")) return;
        int reputation = Integer.parseInt(reputationStr);
        this.value.set(reputation);
        context.write(GROUP_KEY, this.value);
    }
}
