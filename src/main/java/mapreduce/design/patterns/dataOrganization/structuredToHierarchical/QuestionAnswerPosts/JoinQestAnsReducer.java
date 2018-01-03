package mapreduce.design.patterns.dataOrganization.structuredToHierarchical.QuestionAnswerPosts;

import mapreduce.design.patterns.dataOrganization.structuredToHierarchical.AbstractJoinReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public class JoinQestAnsReducer extends AbstractJoinReducer {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setParentFlag("Q");
        setParentName("post");
        setChildName("post");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        if (parent != null) {
            try {
                Text text = new Text(builder.nestMatureElements(parent, children));
                context.write(text, NullWritable.get());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }
}
