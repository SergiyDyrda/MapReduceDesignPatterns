package mapreduce.design.patterns.metapatterns.jobMerging;

import mapreduce.design.patterns.util.XmlBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.util.Random;

import static mapreduce.design.patterns.util.InputRowUtil.defineRow;
import static mapreduce.design.patterns.util.InputRowUtil.getField;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 22.12.2017.
 */

public class AnonymizeDistinctMergedMapper extends Mapper<Object, Text, TaggedText, Text> {
    private XmlBuilder xmlBuilder = new XmlBuilder(DocumentBuilderFactory.newInstance());
    private static final Text DISTINCT_OUT_VALUE = new Text();
    private Random random = new Random();
    private TaggedText anonymizeOutkey = new TaggedText(), distinctOutKey = new TaggedText();
    private Text anonymizeOutValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
        anonymizeMap(key, value, context);
        distinctMap(key, value, context);
    }

    private void anonymizeMap(Object key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        try {
            Element element = xmlBuilder.getXmlElementFromString(row);
            element.removeAttribute("Id");
            element.removeAttribute("UserId");
            String creationDate = element.getAttribute("CreationDate");
            creationDate = creationDate.substring(0, creationDate.indexOf('T'));
            element.setAttribute("CreationDate", creationDate);
            String xmlStringElement = xmlBuilder.transformNodeToString(element);
            anonymizeOutkey.setTag("anon");
            anonymizeOutkey.setText(Integer.toString(random.nextInt()));
            anonymizeOutValue.set(xmlStringElement);
            context.write(anonymizeOutkey, anonymizeOutValue);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void distinctMap(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String row = value.toString().trim();
        if (!row.startsWith("<row")) return;
        defineRow(row, context);
        try {
            String userId = getField("UserId", "UserDisplayName");
            distinctOutKey.setTag("dist");
            distinctOutKey.setText(userId);
            context.write(distinctOutKey, DISTINCT_OUT_VALUE);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }

    }
}
