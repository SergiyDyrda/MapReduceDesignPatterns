package mapreduce.design.patterns.metapatterns.chainFolding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
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

    public static final String ABOVE_OUTPUT_PATH = "aboveAverageReputation";
    public static final String BELOW_OUTPUT_PATH = "belowAverageReputation";

    private String inputPosts = "input/inputPosts.xml";
    private String inputUsers = "input/inputUsers.xml";
    private String output = "output/metapatterns/chainFolding";

    public static void main(String[] args) throws Exception {
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        Job chainFoldingJob = Job.getInstance(conf, "Get binned user reputation with num of posts");

        ChainMapper.addMapper(chainFoldingJob, UserIdCounterMapper.class, LongWritable.class, Text.class, Text.class, LongWritable.class, new Configuration(false));
        ChainMapper.addMapper(chainFoldingJob, UserIdEnrichingMapper.class, Text.class, LongWritable.class, Text.class, LongWritable.class, new Configuration(false));
        ChainReducer.setReducer(chainFoldingJob, LongSumReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, new Configuration(false));
        ChainReducer.addMapper(chainFoldingJob, UserBinningMapper.class, Text.class, LongWritable.class, Text.class, LongWritable.class, new Configuration(false));

        chainFoldingJob.setJarByClass(Driver.class);

        chainFoldingJob.setCombinerClass(LongSumReducer.class);

        chainFoldingJob.setInputFormatClass(TextInputFormat.class);
        chainFoldingJob.setOutputFormatClass(TextOutputFormat.class);

        chainFoldingJob.setOutputKeyClass(Text.class);
        chainFoldingJob.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(chainFoldingJob, new Path(inputPosts));

        MultipleOutputs.addNamedOutput(chainFoldingJob, ABOVE_OUTPUT_PATH, TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(chainFoldingJob, BELOW_OUTPUT_PATH, TextOutputFormat.class, Text.class, LongWritable.class);

        FileOutputFormat.setOutputPath(chainFoldingJob, new Path(output));

        chainFoldingJob.addCacheFile(URI.create(inputUsers));

        return chainFoldingJob.waitForCompletion(true) ? 1 : 0;
    }
}
