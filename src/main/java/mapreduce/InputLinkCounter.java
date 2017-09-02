package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class InputLinkCounter {

    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "localmaster");
        Job job = Job.getInstance(configuration, "InputLinkCounter Job");
        job.setJarByClass(mapreduce.InputLinkCounter.class);
    }
}
