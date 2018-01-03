package mapreduce.design.patterns.summarisation.average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

        final Job job = Job.getInstance(conf, "Get average length of post text by hour");
        job.setJarByClass(Driver.class);

        job.setMapperClass(AverageMapper.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(AverageTuple.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AverageOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input/inputComments.xml"));
        FileOutputFormat.setOutputPath(job, new Path("output/summarisation/average"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
