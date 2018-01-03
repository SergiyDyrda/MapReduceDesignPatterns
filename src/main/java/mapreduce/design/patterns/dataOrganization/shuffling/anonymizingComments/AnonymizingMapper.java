package mapreduce.design.patterns.dataOrganization.shuffling.anonymizingComments;

import mapreduce.design.patterns.util.XmlBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.util.Random;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 15.12.2017.
 */
public class AnonymizingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private XmlBuilder xmlBuilder = new XmlBuilder(DocumentBuilderFactory.newInstance());
    private Random random = new Random();
    private IntWritable outKey = new IntWritable();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
            outKey.set(random.nextInt());
            outValue.set(xmlStringElement);
            context.write(outKey, outValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
