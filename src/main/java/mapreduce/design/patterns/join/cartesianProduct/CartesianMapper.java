package mapreduce.design.patterns.join.cartesianProduct;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import mapreduce.design.patterns.util.XmlBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 19.12.2017.
 */
public class CartesianMapper extends Mapper<Text, Text, Text, Text> {

    private XmlBuilder xmlBuilder = new XmlBuilder(DocumentBuilderFactory.newInstance());
    private Function<String, WrappedString> wrappedStringFunction;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        wrappedStringFunction = new Function<String, WrappedString>() {
            @Override
            public WrappedString apply(@Nullable String o) {
                return wrap(o);
            }
        };
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String keyString = key.toString().trim();
            String valueString = value.toString().trim();
            if (!keyString.startsWith("<row") || !valueString.startsWith("<row")) return;
            Element leftElement = xmlBuilder.getXmlElementFromString(keyString);
            Element rightElement = xmlBuilder.getXmlElementFromString(valueString);
            if (!leftElement.getAttribute("Id").equalsIgnoreCase(rightElement.getAttribute("Id"))) {
                String bodyLeft = leftElement.getAttribute("Body");
                String bodyRight = rightElement.getAttribute("Body");

                HashSet<WrappedString> leftSet = new HashSet<>(Collections2.transform(Arrays.asList(bodyLeft.split("\\s")), wrappedStringFunction));
                HashSet<WrappedString> rightSet = new HashSet<>(Collections2.transform(Arrays.asList(bodyRight.split("\\s")), wrappedStringFunction));
                int wordCount = 0;
                for (WrappedString word : leftSet) {
                    if (rightSet.contains(word)) wordCount++;
                }
                if (wordCount > 12) {
                    context.write(new Text(keyString), new Text(valueString));
                }
            }
        } catch (ParserConfigurationException | SAXException e) {
            e.printStackTrace();
        }
    }

    private WrappedString wrap(String string) {
        return new WrappedString(string);
    }

    private static class WrappedString {
        private String string;

        WrappedString(String string) {
            this.string = string;
        }

        @Override
        public int hashCode() {
            return string.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj || obj instanceof WrappedString && string.equalsIgnoreCase(String.valueOf(obj));
        }

        @Override
        public String toString() {
            return string;
        }
    }
}
