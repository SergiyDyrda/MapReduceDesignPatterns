package mapreduce.design.patterns.dataOrganization.partitioning.usersByLastAccesDate;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getDate;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public class LastAccessDateMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private IntWritable key = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(getDate("LastAccessDate", dateFormat));
            this.key.set(calendar.get(Calendar.YEAR));
            context.write(this.key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
