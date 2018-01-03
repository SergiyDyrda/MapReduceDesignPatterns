package mapreduce.design.patterns.dataOrganization.binning.binningByTags;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public class BinningByTagMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private MultipleOutputs<Text, NullWritable> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String rawTags = getField("Tags");
            String tags = StringEscapeUtils.unescapeHtml(rawTags);
            String[] tagsArray = tags.split("><");
            for (String tag : tagsArray) {
                String groomed = tag.replaceAll("[><]", "").toLowerCase();
                if (groomed.equalsIgnoreCase("hadoop")) {
                    multipleOutputs.write("bins", value, NullWritable.get(), "hadoop-tag");
                }
                if (groomed.equalsIgnoreCase("pig")) {
                    multipleOutputs.write("bins", value, NullWritable.get(), "pig-tag");
                }
                if (groomed.equalsIgnoreCase("hive")) {
                    multipleOutputs.write("bins", value, NullWritable.get(), "hive-tag");
                }
                if (groomed.equalsIgnoreCase("hbase")) {
                    multipleOutputs.write("bins", value, NullWritable.get(), "hbase-tag");
                }
            }
            String post = getField("Body");
            if (post.toLowerCase().contains("hadoop")) {
                multipleOutputs.write("bins", value, NullWritable.get(), "hadoop-post");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
