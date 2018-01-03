package mapreduce.design.patterns.dataOrganization.partitioning.usersByLastAccesDate;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public class LastAccessDatePartitioner extends Partitioner<IntWritable, Text> implements Configurable {
    private static final String LAST_ACCESS_DATE_YEAR = "last.access.date.year";
    private Configuration configuration;
    private int lastAccessYear;

    @Override
    public int getPartition(IntWritable year, Text text, int numPartitions) {
        return (year.get() - lastAccessYear) % numPartitions;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
        lastAccessYear = conf.getInt(LAST_ACCESS_DATE_YEAR, 0);
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void setLastAccessDateYear(Job job, int lastAccessDateYear) {
        job.getConfiguration().setInt(LAST_ACCESS_DATE_YEAR, lastAccessDateYear);
    }
}
