package mapreduce.design.patterns.summarisation.counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static mapreduce.design.patterns.summarisation.counters.CounterMapper.STATE_COUNTER_GROUP;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 12.12.2017.
 */
public class Driver extends Configured implements Tool {

    private static String OUTPUT = "output/summarisation/counters";


    public static void main(String[] args) throws Exception {
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        final Job job = Job.getInstance(conf, "Get number of users by state using hadoop counters");
        job.setJarByClass(Driver.class);

        job.setMapperClass(CounterMapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input/inputUsers.xml"));
        Path outputPath = new Path(OUTPUT);
        FileOutputFormat.setOutputPath(job, outputPath);

        int exit = job.waitForCompletion(true) ? 0 : 1;

        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(outputPath, true);

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(OUTPUT + "/result"));
        for (Counter counter : job.getCounters().getGroup(STATE_COUNTER_GROUP)) {
            String string = counter.getDisplayName() + " - " + counter.getValue();
            System.out.println(string);
            fsDataOutputStream.writeBytes(string);
            fsDataOutputStream.writeBytes("\n");
        }
        fsDataOutputStream.flush();
        fsDataOutputStream.close();

        return exit;
    }

}
