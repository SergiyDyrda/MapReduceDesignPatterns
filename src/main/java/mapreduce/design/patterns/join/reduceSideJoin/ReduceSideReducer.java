package mapreduce.design.patterns.join.reduceSideJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 18.12.2017.
 */
public class ReduceSideReducer extends Reducer<Text, Text, Text, Text> {

    private Text EMPTY_TEXT = new Text("");
    private String joinType;
    private List<String> users = new ArrayList<>();
    private List<String> comments = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        joinType = context.getConfiguration().get(Driver.JOIN_TYPE);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        users.clear();
        comments.clear();

        for (Text value : values) {
            String str = value.toString();
            if (str.startsWith("user")) {
                users.add(str.split("\t")[1]);
            } else {
                comments.add(str.split("\t")[1]);
            }
        }

        executeJoinLogic(context);
    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {
        if (joinType.equalsIgnoreCase(Driver.INNER)) {
            if (!users.isEmpty() && !comments.isEmpty()) {
                for (String user : users) {
                    for (String comment : comments) {
                        context.write(new Text(user), new Text(comment));
                    }
                }
            }
        }
        if (joinType.equalsIgnoreCase(Driver.LEFT_OUTER)) {
            for (String user : users) {
                if (!comments.isEmpty()) {
                    for (String comment : comments) {
                        context.write(new Text(user), new Text(comment));
                    }
                } else {
                    context.write(new Text(user), EMPTY_TEXT);
                }
            }
        }
        if (joinType.equalsIgnoreCase(Driver.RIGHT_OUTER)) {
            for (String comment : comments) {
                if (!users.isEmpty()) {
                    for (String user : users) {
                        context.write(new Text(user), new Text(comment));
                    }
                } else {
                    context.write(EMPTY_TEXT, new Text(comment));
                }
            }
        }
        if (joinType.equalsIgnoreCase(Driver.FULL_OUTER)) {
            if (!users.isEmpty()) {
                for (String user : users) {
                    if (!comments.isEmpty()) {
                        for (String comment : comments) {
                            context.write(new Text(user), new Text(comment));
                        }
                    } else {
                        context.write(new Text(user), EMPTY_TEXT);
                    }
                }
            } else {
                if (!comments.isEmpty()) {
                    for (String comment : comments) {
                        context.write(EMPTY_TEXT, new Text(comment));
                    }
                }
            }
        }
        if (joinType.equalsIgnoreCase(Driver.ANTI_JOIN)) {
            if (users.isEmpty() ^ comments.isEmpty()) {
                for (String user : users) {
                    context.write(new Text(user), EMPTY_TEXT);
                }
                for (String comment : comments) {
                    context.write(EMPTY_TEXT, new Text(comment));
                }
            }
        }
    }
}
