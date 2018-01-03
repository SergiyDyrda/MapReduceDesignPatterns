package mapreduce.design.patterns.join.reduceSideJoin.withBloomFilter;

import mapreduce.design.patterns.join.reduceSideJoin.ReduceSideReducer;
import mapreduce.design.patterns.util.XmlBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by Sergiy Dyrda
 * <p>
 * on 08.12.2017.
 * <p>
 */
public class Driver extends Configured implements Tool {

    public static final String JOIN_TYPE = "inner";

    public static final String INNER = "innerJoin";
    public static final String LEFT_OUTER = "leftOuter";
    public static final String RIGHT_OUTER = "rightOuter";
    public static final String FULL_OUTER = "fullOuter";
    public static final String ANTI_JOIN = "antiJoin";

    private static String bloomFilterPath = "output/temp/bloomFilter";


    public static void main(String[] args) throws Exception {
        int runner = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(runner);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");
        final Configuration conf = this.getConf();
        conf.set("hadoop.tmp.dir", "/home/tmp");

        conf.set(JOIN_TYPE, INNER);

        final Job job = Job.getInstance(conf, "Reduce side join(inner|left|right|full|anti)");
        trainBloomFilter(job);

        job.setJarByClass(Driver.class);

        MultipleInputs.addInputPath(job, new Path("input/inputUsers.xml"), TextInputFormat.class, UserReduceSideMapperWithBloomFilter.class);
        MultipleInputs.addInputPath(job, new Path("input/inputComments.xml"), TextInputFormat.class, CommentReduceSideMapperWithBloomFilter.class);
        job.setReducerClass(ReduceSideReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path("output/join/reduceSide/withBloomFilter"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static void trainBloomFilter(Job job) throws IOException, ParserConfigurationException, SAXException, URISyntaxException {
        BloomFilter filter = new BloomFilter(1000000, 8, Hash.MURMUR_HASH);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FSDataInputStream usersStream = fileSystem.open(new Path("input/inputUsers.xml"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(usersStream));
        XmlBuilder xmlBuilder = new XmlBuilder(DocumentBuilderFactory.newInstance());
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (!line.startsWith("<row")) continue;
            Element element = xmlBuilder.getXmlElementFromString(line);
            String reputation = element.getAttribute("Reputation");
            if (!reputation.isEmpty() && Integer.parseInt(reputation) < 1500) continue;
            String id = element.getAttribute("Id");
            filter.add(new Key(id.getBytes()));
        }
        reader.close();

        FSDataOutputStream stream = fileSystem.create(new Path(bloomFilterPath));
        filter.write(stream);
        stream.flush();
        stream.close();

        job.setCacheFiles(new URI[]{new URI(bloomFilterPath)});

    }
}
