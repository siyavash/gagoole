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

}
