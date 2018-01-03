package mapreduce.design.patterns.inputOutput.generatingData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


/**
 * Created by Sergiy Dyrda
 * <p>
 * on 26.12.2017.
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

        final Job job = Job.getInstance(conf, "Generate random xml stackoverflow comments");
        job.setJarByClass(Driver.class);

        job.setMapperClass(Mapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        RandomCommentsGenerateInputFormat.setNumMapTasks(job, 1);
        RandomCommentsGenerateInputFormat.setNumRecordsPerTask(job, 1000);

        job.setInputFormatClass(RandomCommentsGenerateInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path("output/inputOutput/generatingData"));
        job.addCacheFile(URI.create("input/inOutPatterns/text"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
