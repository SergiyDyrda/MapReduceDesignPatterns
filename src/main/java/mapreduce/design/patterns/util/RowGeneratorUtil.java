package mapreduce.design.patterns.util;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 26.12.2017.
 */
public class RowGeneratorUtil {
    private RowGeneratorUtil() {
    }

    public static void main(String[] args) {
        String dateFormatString = "yyyy-MM-dd'T'HH:mm:ss.SSS";

        DateFormat dateFormat = new SimpleDateFormat(dateFormatString);
        Random random = new Random();
        Map<String, String> map = new LinkedHashMap<>();

        map.put("Id", Integer.toString(random.nextInt(100000)));
        map.put("PostId", Integer.toString(random.nextInt(10000)));
        map.put("Score", Integer.toString(random.nextInt(100)));
        map.put("Text", "This is text");
        map.put("CreationDate", dateFormat.format(new Date(random.nextLong())));

        System.out.println(generateRow("row", map));
    }

    public static String generateRow(String tag, Map<String, String> attributes) {
        StringBuilder builder = new StringBuilder();
        builder.append("<").append(tag).append(" ");
        for (Map.Entry attribute : attributes.entrySet()) {
            builder.append(attribute.getKey())
                    .append("=")
                    .append(wrapped((String) attribute.getValue()));
            builder.append(" ");
        }
        builder.append("/>");
        return builder.toString();
    }

    private static String wrapped(String str) {
        return "\"" + str + "\"";
    }

}
