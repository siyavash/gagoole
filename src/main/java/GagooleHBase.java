import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GagooleHBase
{
    private Connection hbaseConnection;
    private TableName tableName;
    private byte[] columnFamily;

    public GagooleHBase(String tableStringName, String columnFamilyName) throws IOException
    {
        Configuration configuration = HBaseConfiguration.create();
        //set configuration if needed
        hbaseConnection = ConnectionFactory.createConnection(configuration);
        tableName = TableName.valueOf(tableStringName);
        columnFamily = Bytes.toBytes(columnFamilyName);
    }

    public boolean exists(String url, Table table) throws IOException
    {
        Get get = new Get(Bytes.toBytes(url));
        Result result = table.get(get);
        return result.getRow() != null;
    }

    public Table getTable() throws IOException
    {
        return hbaseConnection.getTable(tableName);
    }

    public void put(URLData urlData, Table table) throws IOException
    {
        byte[] urlBytes = Bytes.toBytes(urlData.getUrl());

        put(table, urlBytes, Bytes.toBytes(urlData.getMeta()), Bytes.toBytes("meta")); //TODO column names?
        put(table, urlBytes, Bytes.toBytes(urlData.getPassage()), Bytes.toBytes("passage"));
        put(table, urlBytes, Bytes.toBytes(urlData.getTitle()), Bytes.toBytes("title"));
        put(table, urlBytes, Bytes.toBytes(urlData.getInsideLinks()), Bytes.toBytes("links"));
    }

    private void put(Table table, byte[] urlBytes, byte[] inputBytes, byte[] columnName) throws IOException
    {
        Put put = new Put(urlBytes);
        put.addColumn(columnFamily, columnName, inputBytes);
        table.put(put);
        table.close();
    }
}
