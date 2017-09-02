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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InputLinkCounter {

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

        @Override
        protected void map(ImmutableBytesWritable key,
                           Result value,
                           Context context) throws IOException, InterruptedException {

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
