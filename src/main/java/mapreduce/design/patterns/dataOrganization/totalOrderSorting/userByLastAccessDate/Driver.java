package mapreduce.design.patterns.dataOrganization.totalOrderSorting.userByLastAccessDate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
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

        String out = "output/dataOrganization/totalOrderSorting/accessByDate";

        Path inputPath = new Path("input/inputUsers.xml");
        Path outputStaging = new Path(out + "/_staging");
        Path partitionFile = new Path(out + "/partition.lst");
        Path outputPath = new Path(out);

        final Job analyzeJob = Job.getInstance(conf, "Total order sorting user data set by last access date (analyze phase)");

        analyzeJob.setJarByClass(Driver.class);
        analyzeJob.setMapperClass(AnalyzeMapper.class);
        analyzeJob.setNumReduceTasks(0);

        analyzeJob.setOutputKeyClass(Text.class);
        analyzeJob.setOutputValueClass(Text.class);

        analyzeJob.setInputFormatClass(TextInputFormat.class);
        analyzeJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(analyzeJob, inputPath);
        SequenceFileOutputFormat.setOutputPath(analyzeJob, outputStaging);

        int analyzeJobResult = analyzeJob.waitForCompletion(true) ? 0 : 1;

        if (analyzeJobResult != 0) System.exit(analyzeJobResult);

        final Job orderJob = Job.getInstance(conf, "Total order sorting user data set by last access date (order phase)");

        orderJob.setJarByClass(Driver.class);

        orderJob.setMapperClass(Mapper.class);
        orderJob.setReducerClass(OrderReducer.class);
        orderJob.setNumReduceTasks(4);

        orderJob.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);

        orderJob.setOutputKeyClass(Text.class);
        orderJob.setOutputValueClass(NullWritable.class);

        orderJob.setInputFormatClass(SequenceFileInputFormat.class);
        orderJob.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.setInputPaths(orderJob, outputStaging);
        TextOutputFormat.setOutputPath(orderJob, outputPath);

        orderJob.getConfiguration().set("mapred.textoutputformat.separator", "");

        InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(.001, 10000));

        return orderJob.waitForCompletion(true) ? 0 : 1;
    }
}
