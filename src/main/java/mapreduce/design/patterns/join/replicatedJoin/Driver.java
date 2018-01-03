package mapreduce.design.patterns.join.replicatedJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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

    public static final String JOIN_TYPE = "join.type";

    public static final String INNER = "innerJoin";
    public static final String LEFT_OUTER = "leftOuter";

    public static void main(String[] args) throws Exception {
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        conf.set(JOIN_TYPE, LEFT_OUTER);

        final Job job = Job.getInstance(conf, "Replicated join(inner|left)");
        job.setJarByClass(Driver.class);

        job.setCacheFiles(new URI[]{new URI("input/small-inputUsers.xml")});

        job.setMapperClass(CommentsReplicatedJoinMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input/inputComments.xml"));
        FileOutputFormat.setOutputPath(job, new Path("output/join/replicatedJoin"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
