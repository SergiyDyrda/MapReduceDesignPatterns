package mapreduce.design.patterns.dataOrganization.binning.binningByTags;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
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

    public static final String NAMED_OUTPUT = "output/dataOrganization/binning/byTags";

    public static void main(String[] args) throws Exception {
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        final Job job = Job.getInstance(conf, "bin input posts by tag");
        job.setJarByClass(Driver.class);
        job.setMapperClass(BinningByTagMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input/inputPosts.xml"));
        FileOutputFormat.setOutputPath(job, new Path(NAMED_OUTPUT));
        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
                Text.class, NullWritable.class);

        MultipleOutputs.setCountersEnabled(job, true);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
