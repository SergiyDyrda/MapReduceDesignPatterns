package mapreduce.design.patterns.util;

import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 11.12.2017.
 */

public class Wikipedia {

    private Wikipedia() {
    }

    public static Queue<String> getWikipediaURLs(String html) {
        Queue<String> result = new LinkedList<>();
        Pattern compile = Pattern.compile("<a href=\"([^\"]+)\"");
        Matcher matcher = compile.matcher(html);
        while (matcher.find()) {
            String url = matcher.group(1);
            if (url.contains("wikipedia.")) {
                result.add(url);
            }
        }

        return result;
    }
}
