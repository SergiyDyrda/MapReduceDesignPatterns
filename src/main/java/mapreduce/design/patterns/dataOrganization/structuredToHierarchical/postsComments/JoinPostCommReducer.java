package mapreduce.design.patterns.dataOrganization.structuredToHierarchical.postsComments;

import mapreduce.design.patterns.dataOrganization.structuredToHierarchical.AbstractJoinReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public class JoinPostCommReducer extends AbstractJoinReducer {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setParentFlag("P");
        setParentName("post");
        setChildName("comment");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        if (parent != null) {
            try {
                Text text = new Text(builder.nestElementsFromRawData(parent, children, getParentName(), getChildName()));
                context.write(text, NullWritable.get());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }
}
