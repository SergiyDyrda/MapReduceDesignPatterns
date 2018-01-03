package mapreduce.design.patterns.dataOrganization.partitioning.usersByLastAccesDate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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

        final Job job = Job.getInstance(conf, "shard users by last access date");
        job.setJarByClass(Driver.class);

        job.setMapperClass(LastAccessDateMapper.class);
        job.setReducerClass(LastAccessDateReducer.class);
        job.setPartitionerClass(LastAccessDatePartitioner.class);
        LastAccessDatePartitioner.setLastAccessDateYear(job, 2010);

        //from 2010 to 2013 year
        job.setNumReduceTasks(4);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input/inputUsers.xml"));
        FileOutputFormat.setOutputPath(job, new Path("output/dataOrganization/partitioning/lastAccessDate"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
