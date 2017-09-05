package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AnchorsCounter extends Configured implements Tool {

    private static final byte[] COLUMN_FAMILY = "cf".getBytes();
    private static final byte[] SUB_LINKS = "subLinks".getBytes();

    public static class Mapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
        @Override
        protected void map(ImmutableBytesWritable key,
                           Result value,
                           Context context) throws IOException, InterruptedException {
            try {
                String subLinks = new String(value.getValue(COLUMN_FAMILY, SUB_LINKS));
                if(subLinks.equals("")) {
                    return;
                }

                String[] linkAnchors = subLinks.split("\n");
                for(int i = 0; i < linkAnchors.length; i += 2) {
                    String link = linkAnchors[i];
                    String anchor = linkAnchors[i + 1];
                    if(anchor == null ||
                       anchor.equals("") ||
                       anchor.equals("that") ||
                       anchor.equals("this") ||
                       anchor.equals("link") ||
                       anchor.equals("here")) {
                        continue;
                    }

                    try {
                        context.write(new ImmutableBytesWritable(link.getBytes()),
                                      new ImmutableBytesWritable(anchor.getBytes()));
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, Put> {

        private static final byte[] ANCHORS = "anchors".getBytes();

        @Override
        protected void reduce(ImmutableBytesWritable key,
                              Iterable<ImmutableBytesWritable> values,
                              Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<String, Integer>();
            for(ImmutableBytesWritable value : values) {
                String anchor = new String(value.copyBytes());
                if(map.containsKey(anchor)) {
                    map.put(anchor, map.get(anchor) + 1);
                }
                else {
                    map.put(anchor, 1);
                }
            }

            Iterator it = map.entrySet().iterator();
            StringBuilder anchors = new StringBuilder("\n");
            while(it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();

                anchors.append(pair.getKey());
                anchors.append('\n');
                anchors.append(pair.getValue());
                anchors.append('\n');
            }

            Put put = new Put(key.get());
            put.addColumn(COLUMN_FAMILY, ANCHORS, new String(anchors).getBytes());

            try {
                context.write(null, put);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "AnchorsCounter");
        job.setJarByClass(AnchorsCounter.class);

        Scan scan = new Scan();

        scan.setCaching(500);
        scan.setCacheBlocks(false);

        scan.addColumn(COLUMN_FAMILY, SUB_LINKS);

        TableMapReduceUtil.initTableMapperJob(
                "wb",
                scan,
                AnchorsCounter.Mapper.class,
                ImmutableBytesWritable.class,
                ImmutableBytesWritable.class,
                job);

        TableMapReduceUtil.initTableReducerJob(
                "wb",
                AnchorsCounter.Reducer.class,
                job);

        boolean jobSuccessful = job.waitForCompletion(true);

        if(jobSuccessful) {
            System.out.println("job completed successfully.");
        }
        else {
            System.out.println("job failed!");
        }

        return jobSuccessful ? 1 : 0;
    }

    public static void main(String[] args) {
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConfiguration.set("hbase.zookeeper.quorum", "master,slave");

        try {
            ToolRunner.run(hbaseConfiguration, new AnchorsCounter(), args);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
