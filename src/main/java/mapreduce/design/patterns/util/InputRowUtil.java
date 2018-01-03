package mapreduce.design.patterns.util;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 */
public class InputRowUtil {

    private static Map<String, String> mapOfFields;
    private static TaskAttemptContext context;

    private InputRowUtil() {
    }

    public static void defineRow(String row, TaskAttemptContext mapperContext) {
        mapOfFields = XmlUtil.getMapFromXmlRow(row);
        context = mapperContext;
    }

    public static Date getDate(String fieldName, String formatOfDate) {
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(formatOfDate);
        DateFormat dateFormat = new SimpleDateFormat(formatOfDate);
        String creationDate = mapOfFields.get(fieldName);
        try {
            return dateFormat.parse(creationDate);
        } catch (ParseException | NullPointerException e) {
            Counter counter = context.getCounter(TaskCounter.MAP_INPUT_RECORDS);
            throw new IllegalArgumentException(String.format("incorrect date %s in row number %d", creationDate, counter.getValue()));
        }
    }

    public static String getField(String fieldName, String... alternativeFieldNames) {
        Objects.requireNonNull(fieldName);
        String field = mapOfFields.get(fieldName);
        if (field == null || field.isEmpty()) {
            if (alternativeFieldNames != null && alternativeFieldNames.length > 0) {
                for (int i = 0; i < alternativeFieldNames.length && field == null; i++) {
                    field = mapOfFields.get(alternativeFieldNames[i]);
                }
            }
            if (field == null || field.isEmpty()) {
                Counter counter = context.getCounter(TaskCounter.MAP_INPUT_RECORDS);
                throw new IllegalArgumentException(String.format("incorrect field %s=%s in row number %d", fieldName, field, counter.getValue()));
            }
        }
        return field;
    }
}
