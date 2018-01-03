package mapreduce.design.patterns.join.reduceSideJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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

    public static final String JOIN_TYPE = "join.type";

    public static final String INNER = "innerJoin";
    public static final String LEFT_OUTER = "leftOuter";
    public static final String RIGHT_OUTER = "rightOuter";
    public static final String FULL_OUTER = "fullOuter";
    public static final String ANTI_JOIN = "antiJoin";

    public static void main(String[] args) throws Exception {
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        conf.set(JOIN_TYPE, INNER);

        final Job job = Job.getInstance(conf, "Reduce side join(inner|left|right|full|anti)");
        job.setJarByClass(Driver.class);

        MultipleInputs.addInputPath(job, new Path("input/inputUsers.xml"), TextInputFormat.class, UserReduceSideMapper.class);
        MultipleInputs.addInputPath(job, new Path("input/inputComments.xml"), TextInputFormat.class, CommentReduceSideMapper.class);
        job.setReducerClass(ReduceSideReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path("output/join/reduceSide"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
