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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class InputLinkCounter extends Configured implements Tool {

    private static final byte[] COLUMN_FAMILY = "cf".getBytes();
    private static final byte[] SUB_LINKS = "subLinks".getBytes();
    private static Logger logger = Logger.getLogger(Class.class.getName());

    public static class Mapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

        private static final IntWritable one = new IntWritable(1);

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) {
            try {
                String subLinks = new String(value.getValue(COLUMN_FAMILY, SUB_LINKS));
                if (subLinks.equals("")) {
                    return;
                }

                String[] linkAnchors = subLinks.split("\n");

                for (String linkAnchor : linkAnchors) {
                    String[] linkAndAnchor = linkAnchor.split(" , ");
                    ImmutableBytesWritable link = new ImmutableBytesWritable(linkAndAnchor[0].getBytes());

                    try {
                        context.write(link, one);
                    }
                    catch (Exception e) {
                        logger.error("mapper could not write to context", e);
                        e.printStackTrace();
                    }

                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reducer extends TableReducer<ImmutableBytesWritable, IntWritable, Put> {

        private static final byte[] NUMBER_OF_INPUT_LINKS = "numOfInputLinks".getBytes();

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) {
            if(key.equals(new ImmutableBytesWritable("".getBytes()))) {
                return;
            }

            String rowKey = new String(key.copyBytes());

            long numberOfInputLinks = 0;
            for(IntWritable value : values) {
                numberOfInputLinks += value.get();
            }

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(COLUMN_FAMILY, NUMBER_OF_INPUT_LINKS, Bytes.toBytes(numberOfInputLinks));

            try {
                context.write(null, put);
            }
            catch (Exception e) {
                logger.error("reducer could not write to contest", e);
                e.printStackTrace();
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "InputLinkCounter");
        job.setJarByClass(mapreduce.InputLinkCounter.class);

        Scan scan = new Scan();

        scan.addColumn(COLUMN_FAMILY, SUB_LINKS);

        scan.setCacheBlocks(false);
        scan.setCaching(500);

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

        boolean jobSuccessful = job.waitForCompletion(true);

        if(jobSuccessful) {
            System.out.printf("job completed successfully.");
        }
        else {
            System.out.printf("job failed!");
        }

        return jobSuccessful ? 1 : 0;
    }

    public static void main(String[] args) {
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConfiguration.set("hbase.zookeeper.quorum", "localmaster");

        try {
            ToolRunner.run(hbaseConfiguration, new InputLinkCounter(), args);
        }
        catch (Exception e) {
            logger.fatal("could not run the job!", e);
            e.printStackTrace();
        }
    }
}
