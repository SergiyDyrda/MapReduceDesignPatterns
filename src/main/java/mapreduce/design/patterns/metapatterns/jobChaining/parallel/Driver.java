package mapreduce.design.patterns.metapatterns.jobChaining.parallel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;


/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 * <p>
 */
public class Driver extends Configured implements Tool {


    private String outputPath = "output/metapatterns/jobChaining/parallel";

    public static void main(String[] args) throws Exception {
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        final Job averageAboveJob = Job.getInstance(conf, "Average reputation");
        Path outputDirAbove = new Path(outputPath + "/above");
        configureJob(averageAboveJob, new Path("input/sequential/aboveAverage"), outputDirAbove);

        final Job averageBelowJob = Job.getInstance(conf, "Average reputation");
        Path outputDirBelow = new Path(outputPath + "/below");
        configureJob(averageBelowJob, new Path("input/sequential/belowAverage"), outputDirBelow);

        for (Job job : Arrays.asList(averageAboveJob, averageBelowJob)) {
            job.submit();
        }

        while (!averageBelowJob.isComplete() || !averageAboveJob.isComplete()) {
            Thread.sleep(5000);
        }
        if (averageBelowJob.isSuccessful()) {
            System.out.println("Below average job completed successfully!");
        } else {
            System.out.println("Below average job failed!");
        }
        if (averageAboveJob.isSuccessful()) {
            System.out.println("Above average job completed successfully!");
        } else {
            System.out.println("Above average job failed!");
        }
       return averageBelowJob.isSuccessful() &&
                averageAboveJob.isSuccessful() ? 0 : 1;
    }

    private void configureJob(Job job, Path input, Path outputDir) throws java.io.IOException {
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
    }
}
