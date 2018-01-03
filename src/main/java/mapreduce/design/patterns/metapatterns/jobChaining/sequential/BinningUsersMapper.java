package mapreduce.design.patterns.metapatterns.jobChaining.sequential;

import mapreduce.design.patterns.util.InputRowUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 21.12.2017.
 */
public class BinningUsersMapper extends Mapper<Text, Text, Text, Text> {

    private double averageNumPosts;
    private Map<String, String> usersReputation = new HashMap<>();
    private MultipleOutputs<Text, Text> multipleOutputs = null;
    private Text outValue = new Text();

    public static void setAverageNumPosts(Job job, double averageNumPosts) {
        job.getConfiguration().setDouble(Driver.AVERAGE_NUM_OF_POSTS, averageNumPosts);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String usersPath = context.getCacheFiles()[0].getPath();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream open = fileSystem.open(new Path(usersPath));
        BufferedReader reader = new BufferedReader(new InputStreamReader(open));
        String line;
        while ((line = reader.readLine()) != null) {
            try {
                line = line.trim();
                if (!line.startsWith("<row")) continue;
                InputRowUtil.defineRow(line, context);
                String id = InputRowUtil.getField("Id");
                String reputation = InputRowUtil.getField("Reputation");
                usersReputation.put(id, reputation);
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
        averageNumPosts = context.getConfiguration().getDouble(Driver.AVERAGE_NUM_OF_POSTS, -1);
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        long numOfPosts = Long.parseLong(value.toString().trim());
        outValue.set(String.format("reputation:%s\tnumOfPosts:%d", usersReputation.get(key.toString()), numOfPosts));
        if ((double) numOfPosts >= averageNumPosts) {
            multipleOutputs.write(Driver.ABOVE_OUTPUT_PATH, key, outValue, Driver.ABOVE_OUTPUT_PATH + "/part");
        } else {
            multipleOutputs.write(Driver.BELOW_OUTPUT_PATH, key, outValue, Driver.BELOW_OUTPUT_PATH + "/part");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
