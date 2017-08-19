import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GagooleHBase
{
    private Table table;
    private byte[] columnFamily = Bytes.toBytes("columnFamily");

    public GagooleHBase(String tableStrName) throws IOException
    {
        Configuration configuration = HBaseConfiguration.create();
        Connection hbaseConnection = ConnectionFactory.createConnection(configuration);

        TableName tableName = TableName.valueOf(tableStrName);
        table = hbaseConnection.getTable(tableName);
        System.out.println();
    }

    public boolean exists(String url) throws IOException
    {
        Get get = new Get(Bytes.toBytes(url));
        Result result = table.get(get);
        return result.getRow() != null;
    }

    public void put(URLData urlData) throws IOException
    {
        byte[] urlBytes = Bytes.toBytes(urlData.getUrl());

        put(urlBytes, Bytes.toBytes(urlData.getMeta()), Bytes.toBytes("meta"));
        put(urlBytes, Bytes.toBytes(urlData.getPassage()), Bytes.toBytes("passage"));
        put(urlBytes, Bytes.toBytes(urlData.getTitle()), Bytes.toBytes("title"));
        put(urlBytes, Bytes.toBytes(urlData.getInsideLinks()), Bytes.toBytes("links"));
    }

    private void put(byte[] urlBytes, byte[] inputBytes, byte[] columnName) throws IOException
    {
        Put put = new Put(urlBytes);
        put.addColumn(columnFamily, columnName, inputBytes);
        table.put(put);
    }
}
