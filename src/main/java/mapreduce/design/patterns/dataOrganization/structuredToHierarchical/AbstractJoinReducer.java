package mapreduce.design.patterns.dataOrganization.structuredToHierarchical;

import mapreduce.design.patterns.util.XmlBuilder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public abstract class AbstractJoinReducer extends Reducer<Text, Text, Text, NullWritable> {

    private DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    protected XmlBuilder builder = new XmlBuilder(factory);

    private String parentFlag, parentName, childName;

    protected String parent;
    protected List<String> children = new ArrayList<>();

    public void setParentFlag(String parentFlag) {
        this.parentFlag = parentFlag;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public void setChildName(String childName) {
        this.childName = childName;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        parent = null;
        children.clear();
        for (Text val : values) {
            String str = val.toString();
            if (str.startsWith(parentFlag)) {
                parent = str.substring(1, str.length());
            } else {
                children.add(str.substring(1, str.length()));
            }
        }
    }

    public String getParentName() {
        return parentName;
    }

    public String getChildName() {
        return childName;
    }
}
