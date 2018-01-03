package mapreduce.design.patterns.summarisation.average;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 */
public class AverageOutputFormat extends FileOutputFormat<LongWritable, AverageTuple> {

    @Override
    public RecordWriter<LongWritable, AverageTuple> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Path outputPath = FileOutputFormat.getOutputPath(job);
        Path fullOutputPath = new Path(outputPath, "success_result");
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FSDataOutputStream outputStream = fileSystem.create(fullOutputPath);
        return new AverageRecordWriter(outputStream);
    }

    public static class AverageRecordWriter extends RecordWriter<LongWritable, AverageTuple> {
        private DataOutputStream outputStream;

        AverageRecordWriter(DataOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public void write(LongWritable key, AverageTuple value) throws IOException, InterruptedException {
            outputStream.writeBytes(String.format("%s\t%s", key.toString(), value.toString()));
            outputStream.writeBytes("\r\n");
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            outputStream.flush();
            outputStream.close();
        }
    }
}
