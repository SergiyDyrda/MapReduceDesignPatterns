package mapreduce.design.patterns.join.replicatedJoin;

import mapreduce.design.patterns.util.InputRowUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 18.12.2017.
 */
public class CommentsReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {

    private Map<String, String> users = new HashMap<>();
    private String joinType;

    private Text EMPTY_TEXT = new Text("");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        joinType = context.getConfiguration().get(Driver.JOIN_TYPE);

        URI[] cacheFiles = context.getCacheFiles();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(cacheFiles[0].getPath()))));
        String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.startsWith("<row")) continue;
                InputRowUtil.defineRow(line, context);
                String id = null;
                try {
                    id = InputRowUtil.getField("Id");
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
                users.put(id, line);
            }
            reader.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        InputRowUtil.defineRow(row, context);
        try {
            String userId = InputRowUtil.getField("UserId");
            String userRow = users.get(userId);
            if (userRow != null) {
                context.write(value, new Text(userRow));
            } else if (joinType.equalsIgnoreCase(Driver.LEFT_OUTER)) {
                context.write(value, EMPTY_TEXT);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
