package mapreduce.design.patterns.dataOrganization.structuredToHierarchical.QuestionAnswerPosts;

import mapreduce.design.patterns.util.XmlBuilder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;


/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public class QuestionAnswerPostMapper extends Mapper<LongWritable, Text, Text, Text> {

    private DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    private XmlBuilder xmlBuilder = new XmlBuilder(factory);

    private Text key = new Text();
    private Text value = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String row = value.toString().trim();
            Element element = xmlBuilder.getXmlElementFromString(row);
            String postTypeId = element.getAttribute("PostTypeId");
            if ("1".equals(postTypeId)) {
                this.key.set(element.getAttribute("Id"));
                this.value.set("Q" + row);
            } else {
                this.key.set(element.getAttribute("ParentId"));
                this.value.set("A" + row);
            }
            context.write(this.key, this.value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
