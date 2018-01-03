package mapreduce.design.patterns.dataOrganization.structuredToHierarchical;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public abstract class AbstractMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text key = new Text();
    private Text value = new Text();
    private String label, fieldIdName;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String id = getField(fieldIdName);
            this.key.set(id);
            this.value.set(label + row);
            context.write(this.key, this.value);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setFieldIdName(String fieldIdName) {
        this.fieldIdName = fieldIdName;
    }
}
