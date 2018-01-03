package mapreduce.design.patterns.dataOrganization.structuredToHierarchical.postsComments;

import mapreduce.design.patterns.dataOrganization.structuredToHierarchical.AbstractMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public class CommentMapper extends AbstractMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setLabel("C");
        setFieldIdName("PostId");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
