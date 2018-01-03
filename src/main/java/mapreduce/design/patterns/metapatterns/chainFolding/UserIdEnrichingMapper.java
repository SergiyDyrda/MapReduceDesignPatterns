package mapreduce.design.patterns.metapatterns.chainFolding;

import mapreduce.design.patterns.util.InputRowUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 22.12.2017.
 */
public class UserIdEnrichingMapper extends Mapper<Text, LongWritable, Text, LongWritable> {

    private Map<String, String> usersReputation = new HashMap<>();

    private Text outKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String usersPath = context.getCacheFiles()[0].getPath();
        BufferedReader reader = null;
        try {
            try (FSDataInputStream stream = FileSystem.get(context.getConfiguration()).open(new Path(usersPath))) {
                reader = new BufferedReader(new InputStreamReader(stream));
            }
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.startsWith("<row")) return;
                InputRowUtil.defineRow(line, context);
                try {
                    String id = InputRowUtil.getField("Id");
                    String reputation = InputRowUtil.getField("Reputation");
                    usersReputation.put(id, reputation);
                } catch (Exception e) {
                    System.err.println("setup:" + e.getMessage());
                }
            }
        } finally {
          if (reader != null) {
              reader.close();
          }
        }

    }

    @Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        String reputation = usersReputation.get(key.toString());
        if (reputation != null) {
            outKey.set(String.format("%s\t%s", key.toString(), reputation));
            context.write(outKey, value);
        }
    }
}
