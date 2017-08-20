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
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master,slave");
        //set configuration if needed
        createConnection(configuration);
        tableName = TableName.valueOf(tableStringName);
        columnFamily = Bytes.toBytes(columnFamilyName);
    }

    private Table createTable(TableName tableName)
    {
        Table table;

        while (true)
        {
            try
            {
                table = hbaseConnection.getTable(tableName);
                return table;
            } catch (IOException ignored)
            {

            }
        }
    }

    private void createConnection(Configuration configuration) throws IOException
    {
        for (int i = 0; i < 50; i++)
        {
            try
            {
                hbaseConnection = ConnectionFactory.createConnection(configuration);
                return;
            } catch (IOException ignored)
            {

            }
        }

        throw new IOException();
    }

    public boolean exists(String url, Table table) throws IOException
    {
        Get get = new Get(Bytes.toBytes(url));
        Result result = getResultFromTable(get, table);
        return result.getRow() != null;
    }

    private Result getResultFromTable(Get get, Table table)
    {
        Result result;

        while (true)
        {
            try
            {
                result = table.get(get);
                return result;
            } catch (IOException ignored)
            {

            }
        }
    }

    public Table getTable() throws IOException
    {
        return createTable(tableName);
    }

    public void put(URLData urlData, Table table)
    {
        byte[] urlBytes = Bytes.toBytes(urlData.getUrl());

        Put put = new Put(urlBytes);

        put.addColumn(columnFamily, Bytes.toBytes("meta"), Bytes.toBytes(urlData.getMeta()));
        put.addColumn(columnFamily, Bytes.toBytes("passage"), Bytes.toBytes(urlData.getPassage()));
        put.addColumn(columnFamily, Bytes.toBytes("title"), Bytes.toBytes(urlData.getTitle()));
        put.addColumn(columnFamily, Bytes.toBytes("links"), Bytes.toBytes(urlData.getInsideLinks()));

        putChangesToTable(put, table);
        closeTable(table);
    }

    private void closeTable(Table table)
    {
        while (true)
        {
            try
            {
                table.close();
                return;
            } catch (IOException ignored)
            {

            }
        }
    }

    private void putChangesToTable(Put put, Table table)
    {
        while (true)
        {
            try
            {
                table.put(put);
                return;
            } catch (IOException ignored)
            {

            }
        }
    }
}
