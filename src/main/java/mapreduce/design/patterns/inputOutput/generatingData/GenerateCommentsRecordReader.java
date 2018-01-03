package mapreduce.design.patterns.inputOutput.generatingData;

import mapreduce.design.patterns.util.RowGeneratorUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 26.12.2017.
 */
public class GenerateCommentsRecordReader extends RecordReader<Text, NullWritable> {
    private long numRecordsPerTask, recordsCreated;
    private List<String> words;

    private Text key = new Text();
    private NullWritable value = NullWritable.get();

    private Random random = new Random();
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        numRecordsPerTask = context.getConfiguration().getLong(RandomCommentsGenerateInputFormat.NUM_RECORDS_PER_TASK, 10);
        words = new ArrayList<>();
        String pathToWords = context.getCacheFiles()[0].getPath();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(pathToWords))))) {
            String line;
            while ((line = reader.readLine())!= null) {
                words.addAll(Arrays.asList(line.split("\\s")));
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (recordsCreated < numRecordsPerTask) {
            Map<String, String> map = new LinkedHashMap<>();

            map.put("Id", Integer.toString(random.nextInt(100000)));
            map.put("PostId", Integer.toString(random.nextInt(10000)));
            map.put("Score", Integer.toString(random.nextInt(100)));
            map.put("Text", generateText(words, 30));
            map.put("CreationDate", dateFormat.format(new Date(random.nextLong())));
            map.put("UserId", Integer.toString(random.nextInt(100000)));

            String key = RowGeneratorUtil.generateRow("row", map);
            this.key.set(key);
            ++recordsCreated;
            return true;
        }
        return false;
    }

    private String generateText(List<String> words, int maximum) {
        int numWords = random.nextInt(maximum) + 1;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numWords; i++) {
            builder.append(words.get(random.nextInt(words.size())));
            builder.append(" ");
        }
        return builder.toString().trim();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) recordsCreated / (float) numRecordsPerTask;
    }

    @Override
    public void close() throws IOException {

    }
}
