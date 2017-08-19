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
}
