package mapreduce.design.patterns.join.compositeJoin.sortJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 18.12.2017.
 */
public class SortByKeyMapper extends Mapper<Object, Text, Text, Text> {

    public static final String FIELD_ID_NAME = "field.id.name";

    private String fieldIdName;

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fieldIdName = context.getConfiguration().get(FIELD_ID_NAME);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String field = getField(fieldIdName);
            outKey.set(field);
            outValue.set(row);
            context.write(outKey, outValue);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void setFieldIdName(String fieldIdName, Job job) {
        job.getConfiguration().set(FIELD_ID_NAME, fieldIdName);
    }
}
