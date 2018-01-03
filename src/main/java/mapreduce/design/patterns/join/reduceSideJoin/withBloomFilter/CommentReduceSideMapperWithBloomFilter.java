package mapreduce.design.patterns.join.reduceSideJoin.withBloomFilter;

import mapreduce.design.patterns.join.reduceSideJoin.AbstractReduceSideMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 18.12.2017.
 */
public class CommentReduceSideMapperWithBloomFilter extends AbstractReduceSideMapper {

    private BloomFilter filter = new BloomFilter();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tag = "comment";
        field = "UserId";

        URI[] cacheFiles = context.getCacheFiles();
        DataInputStream stream = new DataInputStream(new FileInputStream(new File(cacheFiles[0].getPath())));
        filter.readFields(stream);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String userId = getField(field);
            if (!filter.membershipTest(new Key(userId.getBytes()))) return;

            outKey.set(userId);
            outValue.set(tag + "\t" + row);
            context.write(outKey, outValue);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
