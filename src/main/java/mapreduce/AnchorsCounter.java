package mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class AnchorsCounter extends Configured implements Tool {

    private static final byte[] COLUMN_FAMILY = "cf".getBytes();
    private static final byte[] SUB_LINKS = "subLinks".getBytes();

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "AnchorsCounter");
        job.setJarByClass(AnchorsCounter.class);

        Scan scan = new Scan();

        scan.setCaching(500);
        scan.setCacheBlocks(false);

        scan.addColumn(COLUMN_FAMILY, SUB_LINKS);

        
        return 0;
    }
}
