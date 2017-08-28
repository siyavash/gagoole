package datastore;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class PageInfoDataStore implements DataStore
{
    private Connection hbaseConnection;
    private static final TableName TABLE_NAME = TableName.valueOf("wb");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
    private ArrayBlockingQueue<Put> putArrayBlockingQueue = new ArrayBlockingQueue<>(10000);
    private Logger logger = Logger.getLogger(Class.class.getName());

    public PageInfoDataStore(String zookeeperClientPort, String zookeeperQuorum) throws IOException
    {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        hbaseConnection = ConnectionFactory.createConnection(configuration);
        startPuttingToTable();
    }

    public boolean exists(String url) throws IOException
    {
        Table table = null;
        boolean result = true;
        try {
            table = hbaseConnection.getTable(TABLE_NAME);
            Get get = new Get(Bytes.toBytes(url));
            result = table.exists(get);
        } finally {
            if (table != null)
                table.close();
        }
        return result;
    }

    public void put(PageInfo pageInfo) throws IOException
    {
        try {
            String subLinks = turnSubLinksToString(pageInfo.getSubLinks());

            byte[] urlBytes = Bytes.toBytes(pageInfo.getUrl());
            Put put = new Put(urlBytes);

            addColumnToPut(put, Bytes.toBytes("authorMeta"), pageInfo.getAuthorMeta());
            addColumnToPut(put, Bytes.toBytes("descriptionMeta"), pageInfo.getDescriptionMeta());
            addColumnToPut(put, Bytes.toBytes("titleMeta"), pageInfo.getTitleMeta());
            addColumnToPut(put, Bytes.toBytes("contentTypeMeta"), pageInfo.getContentTypeMeta());
            addColumnToPut(put, Bytes.toBytes("keyWordsMeta"), pageInfo.getKeyWordsMeta());
            addColumnToPut(put, Bytes.toBytes("bodyText"), pageInfo.getBodyText());
            addColumnToPut(put, Bytes.toBytes("title"), pageInfo.getTitle());
            addColumnToPut(put, Bytes.toBytes("subLinks"), subLinks);

            putArrayBlockingQueue.put(put);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private void addColumnToPut(Put put, byte[] columnName, String value)
    {
        if (value == null)
        {
            return;
        }

        put.addColumn(COLUMN_FAMILY, columnName, Bytes.toBytes(value));
    }

    private String turnSubLinksToString(ArrayList<Pair<String, String>> subLinks)
    {
        if (subLinks == null)
        {
            return null;
        }

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

    private void startPuttingToTable()
    {
        new Thread(() -> {
            long t1, t2 = 0;

            while (true)
            {
                ArrayList<Put> puts = new ArrayList<>();
                t2 = System.currentTimeMillis() - t2;
                logger.info("Started adding Put classes in a list after " + t2 + " milli seconds");
                for (int i = 0; i < 200; i++)
                {
                    try
                    {
                        puts.add(putArrayBlockingQueue.take());
                    } catch (InterruptedException e)
                    {

                    }
                }

                Table table = null;

                try
                {
                    table = hbaseConnection.getTable(TABLE_NAME);
                    t1 = System.currentTimeMillis();
                    table.put(puts);
                    t1 = System.currentTimeMillis() - t1;
                    logger.info("100 put done in " + t1 + " milli seconds");
                    t2 = System.currentTimeMillis();
                } catch (IOException e)
                {
                    logger.error("Failed to put in hbase");
                } finally
                {
                    if (table != null)
                    {

                        try
                        {
                            table.close();
                        } catch (IOException e)
                        {
                            logger.error("Failed to close the table");
                        }
                    }
                }
            }
        }).start();
    }
}
