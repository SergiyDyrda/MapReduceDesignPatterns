package mapreduce.design.patterns.summarisation.median.standartdeviation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getDate;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 */
public class MedianStdDevMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

    private IntWritable hour = new IntWritable();
    private LongWritable textLength = new LongWritable();
    private String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            Date creationDate = getDate("CreationDate", dateFormat);
            this.hour.set(creationDate.getHours());
            this.textLength.set(getField("Text").length());
            context.write(hour, textLength);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
