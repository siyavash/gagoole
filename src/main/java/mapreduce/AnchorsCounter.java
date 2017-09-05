package mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

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
