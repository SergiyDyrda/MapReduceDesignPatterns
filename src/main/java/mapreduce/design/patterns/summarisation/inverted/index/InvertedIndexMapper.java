package mapreduce.design.patterns.summarisation.inverted.index;

import mapreduce.design.patterns.util.Wikipedia;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Queue;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 11.12.2017.
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text url = new Text();
    private Text id = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String postTypeId = getField("PostTypeId");
            if (postTypeId.equals("1")) return;

            String id = getField("Id");
            String body = getField("Body");

            Queue<String> wikipediaURLs = Wikipedia.getWikipediaURLs(StringEscapeUtils.unescapeHtml(body));
            while (!wikipediaURLs.isEmpty()) {
                this.url.set(wikipediaURLs.poll());
                this.id.set(id);
                context.write(this.url, this.id);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
