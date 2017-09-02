package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;

public class InputLinkCounter {

    private static final byte[] COLUMN_FAMILY = "cf".getBytes();

    public static void main(String[] args) throws IOException {
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConfiguration.set("hbase.zookeeper.quorum", "localmaster");
        Job job = Job.getInstance(hbaseConfiguration, "InputLinkCounter Job");
        job.setJarByClass(mapreduce.InputLinkCounter.class);

        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(
                "wb",
                scan,
                Mapper.class,
                ImmutableBytesWritable.class,
                IntWritable.class,
                job);

        TableMapReduceUtil.initTableReducerJob(
                "wb",
                Reducer.class,
                job);


    }

    public static class Mapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

        private static final byte[] SUB_LINKS = "subLinks".getBytes();
        private static final IntWritable one = new IntWritable(1);

        @Override
        protected void map(ImmutableBytesWritable key,
                           Result value,
                           Context context) throws IOException, InterruptedException {
            String subLinks = new String(value.getValue(COLUMN_FAMILY, SUB_LINKS));
            if(subLinks.equals("")) {
                return;
            }

            String[] linkAnchors = subLinks.split("\n");

            for(String linkAnchor : linkAnchors) {
                String[] linkAndAnchor = linkAnchor.split(" , ");
                ImmutableBytesWritable link = new ImmutableBytesWritable(linkAndAnchor[0].getBytes());

                context.write(link, one);
            }
        }
    }

    public static class Reducer extends TableReducer<ImmutableBytesWritable, InputLinkCounter, Put> {

        @Override
        protected void reduce(ImmutableBytesWritable key,
                              Iterable<InputLinkCounter> values,
                              Context context) throws IOException, InterruptedException {

        }
    }
}
