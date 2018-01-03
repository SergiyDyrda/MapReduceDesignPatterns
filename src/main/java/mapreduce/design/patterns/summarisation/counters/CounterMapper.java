package mapreduce.design.patterns.summarisation.counters;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 12.12.2017.
 */
public class CounterMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
    public static final String STATE_COUNTER_GROUP = "State";
    public static final String UNKNOWN_COUNTER = "Unknown";
    public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";
    private String[] statesArray = new String[]{"AL", "AK", "AZ", "AR",
            "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
            "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
            "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
            "OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
            "VT", "VA", "WA", "WV", "WI", "WY"};

    private Set<String> states;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        states = new HashSet<>(Arrays.asList(statesArray));
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String location = getField("Location");
            for (String l : location.split("\\s")) {
                    if (states.contains(l)) {
                        context.getCounter(STATE_COUNTER_GROUP, l).increment(1);
                        return;
                    }
            }
            context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
        } catch (IllegalArgumentException e) {
            context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER).increment(1);
        }
    }
}
