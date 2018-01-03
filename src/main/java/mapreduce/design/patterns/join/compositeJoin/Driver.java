package mapreduce.design.patterns.join.compositeJoin;

import com.google.common.collect.Lists;
import mapreduce.design.patterns.join.compositeJoin.compositeJob.CompositeMapper;
import mapreduce.design.patterns.join.compositeJoin.sortJob.SortByKeyMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
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

    public static final String INNER = "inner";

    private static String outputPath = "output/join/compositeJoin";

    public static void main(String[] args) throws Exception {
        String joinExpression = CompositeInputFormat.compose(INNER, KeyValueTextInputFormat.class, new Path(outputPath + "/users"), new Path(outputPath + "/comments"));
        System.out.println(joinExpression);
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        final Job sortUsersJob = Job.getInstance(conf, "Composite join(inner|outer) -> sort users");
        Path outputDirUsers = new Path(outputPath + "/users");
        configureJob(sortUsersJob, "Id", new Path("input/inputUsers.xml"), outputDirUsers);

        final Job sortCommentsJob = Job.getInstance(conf, "Composite join(inner|outer) -> sort comments");
        Path outputDirComments = new Path(outputPath + "/comments");
        configureJob(sortCommentsJob, "UserId", new Path("input/inputComments.xml"), outputDirComments);

        final Job compositeJoinJob = Job.getInstance(conf, "Composite join(inner|outer) -> final join");
        compositeJoinJob.getConfiguration().set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
        String joinExpression = CompositeInputFormat.compose(INNER, KeyValueTextInputFormat.class, outputDirUsers, outputDirComments);
        compositeJoinJob.getConfiguration().set(CompositeInputFormat.JOIN_EXPR, joinExpression);

        compositeJoinJob.setJarByClass(Driver.class);
        compositeJoinJob.setMapperClass(CompositeMapper.class);
        compositeJoinJob.setNumReduceTasks(0);

        compositeJoinJob.setOutputKeyClass(Text.class);
        compositeJoinJob.setOutputValueClass(Text.class);

        compositeJoinJob.setInputFormatClass(CompositeInputFormat.class);
        compositeJoinJob.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(compositeJoinJob, new Path(outputPath + "/finalJoinResult"));


        int exitStatus = 0;
        for (Job job : Lists.newArrayList(sortUsersJob, sortCommentsJob, compositeJoinJob)) {
            boolean jobStatus = job.waitForCompletion(true);
            if (!jobStatus) {
                System.err.println("Error with job " + job.getJobName() + " " + job.getStatus().getFailureInfo());
                exitStatus = 1;
                break;
            }
        }
        return exitStatus;
    }

    private void configureJob(Job job, String fieldId, Path input, Path outputDir) throws java.io.IOException {
        job.setJarByClass(Driver.class);

        job.setMapperClass(SortByKeyMapper.class);
        SortByKeyMapper.setFieldIdName(fieldId, job);

        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);
    }
}
