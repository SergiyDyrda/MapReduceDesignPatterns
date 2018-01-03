package mapreduce.design.patterns.metapatterns.jobChaining.sequential;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 * <p>
 */
public class Driver extends Configured implements Tool {

    public static final String AVERAGE_COUNTER_GROUP = "avg.cnt.group";
    public static final String RECORDS_COUNTER = "Records";
    public static final String USERS_COUNTER = "Users";

    public static final String AVERAGE_NUM_OF_POSTS = "avg.num.posts";
    public static final String ABOVE_OUTPUT_PATH = "aboveAverage";
    public static final String BELOW_OUTPUT_PATH = "belowAverage";

    private String inputPosts = "input/inputPosts.xml";
    private String inputUsers = "input/inputUsers.xml";
    private String output = "output/metapatterns/jobChaining/sequential";
    private String intermediateOutput = output + "_intermediate";

    public static void main(String[] args) throws Exception {
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        final Job countingJob = Job.getInstance(conf, "Count number of users and posts");
        configureCountingJob(countingJob);

        int exitStatus = countingJob.waitForCompletion(true) ? 0 : 1;

        if (exitStatus == 1) {
            System.err.println("Error with job " + countingJob.getJobName() + " " + countingJob.getStatus().getFailureInfo());
            return exitStatus;
        }

        Counter recordsCounter = countingJob.getCounters().findCounter(AVERAGE_COUNTER_GROUP, RECORDS_COUNTER);
        Counter usersCounter = countingJob.getCounters().findCounter(AVERAGE_COUNTER_GROUP, USERS_COUNTER);

        double avgNumOfPosts = (double) recordsCounter.getValue() / (double) usersCounter.getValue();

        final Job binningUsersJob = Job.getInstance(conf, "Bin users by below/above average num of posts");
        configureBinningJob(avgNumOfPosts, binningUsersJob);

        exitStatus = binningUsersJob.waitForCompletion(true) ? 0 : 1;
        if (exitStatus == 1) {
            System.err.println("Error with job " + binningUsersJob.getJobName() + " " + binningUsersJob.getStatus().getFailureInfo());
            return exitStatus;
        }

        FileSystem.get(conf).delete(new Path(intermediateOutput), true);
        return exitStatus;
    }

    private void configureBinningJob(double avgNumOfPosts, Job binningUsersJob) throws java.io.IOException {
        binningUsersJob.setJarByClass(Driver.class);

        binningUsersJob.setMapperClass(BinningUsersMapper.class);
        BinningUsersMapper.setAverageNumPosts(binningUsersJob, avgNumOfPosts);
        binningUsersJob.setNumReduceTasks(0);

        binningUsersJob.setOutputKeyClass(Text.class);
        binningUsersJob.setOutputValueClass(Text.class);

        binningUsersJob.setInputFormatClass(KeyValueTextInputFormat.class);
        binningUsersJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(binningUsersJob, new Path(intermediateOutput));

        MultipleOutputs.addNamedOutput(binningUsersJob, ABOVE_OUTPUT_PATH, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(binningUsersJob, BELOW_OUTPUT_PATH, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.setCountersEnabled(binningUsersJob, true);

        FileOutputFormat.setOutputPath(binningUsersJob, new Path(output));

        binningUsersJob.addCacheFile(URI.create(inputUsers));
    }

    private void configureCountingJob(Job countingJob) throws java.io.IOException {
        countingJob.setJarByClass(Driver.class);

        countingJob.setMapperClass(CountingPostsUsersMapper.class);
        countingJob.setCombinerClass(LongSumReducer.class);
        countingJob.setReducerClass(CountingPostsUsersReducer.class);

        countingJob.setOutputKeyClass(Text.class);
        countingJob.setOutputValueClass(LongWritable.class);

        countingJob.setInputFormatClass(TextInputFormat.class);
        countingJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(countingJob, new Path(inputPosts));
        FileOutputFormat.setOutputPath(countingJob, new Path(intermediateOutput));
    }
}
