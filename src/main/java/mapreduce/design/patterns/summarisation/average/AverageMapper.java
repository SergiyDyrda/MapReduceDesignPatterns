package mapreduce.design.patterns.summarisation.average;

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
public class AverageMapper extends Mapper<LongWritable, Text, LongWritable, AverageTuple> {
    private LongWritable hour = new LongWritable();
    private AverageTuple tuple = new AverageTuple();

    private String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            Date creationDate = getDate("CreationDate", dateFormat);
            String text = getField("Text");
            hour.set(creationDate.getHours());
            tuple.setCount(1).setAverage(text.length());
            context.write(hour, tuple);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
