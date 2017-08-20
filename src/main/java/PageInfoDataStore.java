import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PageInfoDataStore
{
    private Connection hbaseConnection;
    private TableName tableName;
    private byte[] columnFamily;

    public PageInfoDataStore(String tableName, String columnFamilyName) throws IOException
    {
        //TODO get configuration data in constructor
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master,slave");
        hbaseConnection = ConnectionFactory.createConnection(configuration);
        this.tableName = TableName.valueOf(tableName);
        columnFamily = Bytes.toBytes(columnFamilyName);
    }

    private Table createTable(TableName tableName) //TODO createTable method is useless
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

    public boolean exists(String url, Table table) throws IOException //TODO don't get table in input
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

    public void put(PageInfo pageInfo, Table table)
    {
        byte[] urlBytes = Bytes.toBytes(pageInfo.getUrl());
        Put put = new Put(urlBytes);


        put.addColumn(columnFamily, Bytes.toBytes("meta"), Bytes.toBytes(pageInfo.getMeta()));
        put.addColumn(columnFamily, Bytes.toBytes("passage"), Bytes.toBytes(pageInfo.getBodyText()));
        put.addColumn(columnFamily, Bytes.toBytes("title"), Bytes.toBytes(pageInfo.getTitle()));
        put.addColumn(columnFamily, Bytes.toBytes("links"), Bytes.toBytes(pageInfo.getInsideLinks()));

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

    private void putChangesToTable(Put put, Table table) //
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
