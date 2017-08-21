import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class PageInfoDataStore
{
    private Connection hbaseConnection;
    private TableName tableName = TableName.valueOf("smallTable");
    private byte[] columnFamily = Bytes.toBytes("columnTable");

    public PageInfoDataStore(String zookeeperClientPort, String zookeeperQuorum) throws IOException
    {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        hbaseConnection = ConnectionFactory.createConnection(configuration);
    }

    public PageInfoDataStore() throws IOException
    {
        Configuration configuration = HBaseConfiguration.create();
        hbaseConnection = ConnectionFactory.createConnection(configuration);
    }

    public boolean exists(String url) throws IOException
    {
        Table table = hbaseConnection.getTable(tableName);
        Get get = new Get(Bytes.toBytes(url));
        Result result = table.get(get);
        return result.getRow() != null;
    }

    public void put(PageInfo pageInfo) throws IOException
    {
        String subLinks = turnSubLinksToString(pageInfo.getSubLinks());

        byte[] urlBytes = Bytes.toBytes(pageInfo.getUrl());
        Put put = new Put(urlBytes);

        Table table = hbaseConnection.getTable(tableName);

        addColumnToPut(put, Bytes.toBytes("meta"), pageInfo.getMeta());
        addColumnToPut(put, Bytes.toBytes("bodyText"), pageInfo.getBodyText());
        addColumnToPut(put, Bytes.toBytes("title"), pageInfo.getTitle());
        addColumnToPut(put, Bytes.toBytes("subLinks"), subLinks);

        table.put(put);
        table.close();
    }

    private void addColumnToPut(Put put, byte[] columnName, String value)
    {
        if (value == null)
        {
            return;
        }

        put.addColumn(columnFamily, columnName, Bytes.toBytes(value));
    }

    private String turnSubLinksToString(ArrayList<Pair<String, String>> subLinks)
    {
        StringBuilder stringBuilder = new StringBuilder();

        for (Pair<String, String> subLink : subLinks)
        {
            String linkName = "";
            if (subLink.getKey() != null)
            {
                linkName = subLink.getKey();
            }

            String anchorName = "";
            if (subLink.getValue() != null)
            {
                anchorName = subLink.getValue();
            }


            stringBuilder.append(linkName);
            stringBuilder.append(" , ");
            stringBuilder.append(anchorName);
            stringBuilder.append("\n");
        }

        return stringBuilder.toString();
    }
}
