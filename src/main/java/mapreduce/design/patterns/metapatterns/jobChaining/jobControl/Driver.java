package mapreduce.design.patterns.metapatterns.jobChaining.jobControl;

import mapreduce.design.patterns.metapatterns.jobChaining.parallel.AverageReputationMapper;
import mapreduce.design.patterns.metapatterns.jobChaining.parallel.AverageReputationReducer;
import mapreduce.design.patterns.metapatterns.jobChaining.sequential.BinningUsersMapper;
import mapreduce.design.patterns.metapatterns.jobChaining.sequential.CountingPostsUsersMapper;
import mapreduce.design.patterns.metapatterns.jobChaining.sequential.CountingPostsUsersReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.Collections;

import static mapreduce.design.patterns.metapatterns.jobChaining.sequential.Driver.*;


/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 * <p>
 */
public class Driver extends Configured implements Tool {

    private String inputPosts = "input/inputPosts.xml";
    private String inputUsers = "input/inputUsers.xml";

    private String outputPath = "output/metapatterns/jobChaining/jobControl";

    private String intermediateOutputCountingJob = outputPath + "_counting";
    private String intermediateOutputBinningJob = outputPath + "_binning";

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

        int exitCode = 0;
        if (countingJob.waitForCompletion(true)) {
            Counter recordsCounter = countingJob.getCounters().findCounter(AVERAGE_COUNTER_GROUP, RECORDS_COUNTER);
            Counter usersCounter = countingJob.getCounters().findCounter(AVERAGE_COUNTER_GROUP, USERS_COUNTER);
            double avgNumOfPosts = (double) recordsCounter.getValue() / (double) usersCounter.getValue();

            ControlledJob binningControlledJob = new ControlledJob(configureBinningJob(conf, avgNumOfPosts),
                    Collections.<ControlledJob>emptyList());

            Path outputDirAbove = new Path(outputPath + "/above");
            ControlledJob aboveAvgControlledJob = new ControlledJob(configureAverageJob(conf, new Path(intermediateOutputBinningJob + "/" + ABOVE_OUTPUT_PATH), outputDirAbove),
                    Collections.singletonList(binningControlledJob));

            Path outputDirBelow = new Path(outputPath + "/below");
            ControlledJob belowAvgControlledJob = new ControlledJob(configureAverageJob(conf, new Path(intermediateOutputBinningJob + "/" + BELOW_OUTPUT_PATH), outputDirBelow),
                    Collections.singletonList(binningControlledJob));


            JobControl jc = new JobControl("AverageReputation");
            jc.addJob(binningControlledJob);
            jc.addJob(aboveAvgControlledJob);
            jc.addJob(belowAvgControlledJob);
            jc.run();
            exitCode = jc.getFailedJobList().size() == 0 ? 0 : 1;
        }

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(intermediateOutputCountingJob), true);
        fs.delete(new Path(intermediateOutputBinningJob), true);

        return exitCode;
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
        FileOutputFormat.setOutputPath(countingJob, new Path(intermediateOutputCountingJob));
    }

    private Job configureBinningJob(Configuration conf, double avgNumOfPosts) throws java.io.IOException {
        Job binningUsersJob = Job.getInstance(conf, "Bin users by below/above average num of posts");
        binningUsersJob.setJarByClass(Driver.class);

        binningUsersJob.setMapperClass(BinningUsersMapper.class);
        BinningUsersMapper.setAverageNumPosts(binningUsersJob, avgNumOfPosts);
        binningUsersJob.setNumReduceTasks(0);

        binningUsersJob.setOutputKeyClass(Text.class);
        binningUsersJob.setOutputValueClass(Text.class);

        binningUsersJob.setInputFormatClass(KeyValueTextInputFormat.class);
        binningUsersJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(binningUsersJob, new Path(intermediateOutputCountingJob));

        MultipleOutputs.addNamedOutput(binningUsersJob, ABOVE_OUTPUT_PATH, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(binningUsersJob, BELOW_OUTPUT_PATH, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.setCountersEnabled(binningUsersJob, true);

        FileOutputFormat.setOutputPath(binningUsersJob, new Path(intermediateOutputBinningJob));

        binningUsersJob.addCacheFile(URI.create(inputUsers));
        return binningUsersJob;
    }

    private Job configureAverageJob(Configuration conf, Path input, Path outputDir) throws java.io.IOException {
        Job job = Job.getInstance(conf, "Average reputation");
        job.setJarByClass(Driver.class);

        job.setMapperClass(AverageReputationMapper.class);
        job.setReducerClass(AverageReputationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        return job;
    }
}
