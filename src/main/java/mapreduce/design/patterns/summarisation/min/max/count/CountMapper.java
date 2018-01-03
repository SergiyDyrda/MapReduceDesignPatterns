package mapreduce.design.patterns.summarisation.min.max.count;

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

class CountMapper extends Mapper<LongWritable, Text, Text, MinMaxCountTuple> {

    private Text key = new Text();
    private MinMaxCountTuple tuple = new MinMaxCountTuple();
    private String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            this.key.set(getField("UserId", "UserDisplayName"));
            Date creationDate = getDate("CreationDate", dateFormat);
            this.tuple.setFirst(creationDate).setLast(creationDate).setCount(1);
            context.write(this.key, tuple);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }




}

