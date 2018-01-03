package mapreduce.design.patterns.util;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 07.12.2017.
 */

public class XmlUtil {

    public static Map<String, String> getMapFromXmlRow(String row) {
        Map<String, String> resultMap = new HashMap<>();
        row = row.replaceAll("<[\\w\\d]+|\\s/>", "");
        Pattern pattern = Pattern.compile("((\\w+)=\"([^\"]+)\")");
        Matcher matcher = pattern.matcher(row);
        while (matcher.find()) {
            resultMap.put(matcher.group(2), matcher.group(3));
        }

        return resultMap;
    }
}
