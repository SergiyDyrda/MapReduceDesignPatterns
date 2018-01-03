package mapreduce.design.patterns.summarisation.median.standartdeviation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 * <p>
 * Problem: Given a list of user’s comments, determine the median and standard deviation
 * of comment lengths per hour of day.
 */
public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        final Job job = Job.getInstance(conf, "Get median and standard deviation of comments length by hour");
        job.setJarByClass(Driver.class);

        job.setMapperClass(MedianStdDevMapper.class);
        job.setReducerClass(MedianStdDevReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedianStdDevTuple.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(MedianStdDevOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input/inputComments.xml"));
        FileOutputFormat.setOutputPath(job, new Path("output/summarisation/medianStdDev"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
