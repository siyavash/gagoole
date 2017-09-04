package datastore;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import util.Profiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class PageInfoDataStore implements DataStore
{
    private Connection hbaseConnection;
    private static final TableName TABLE_NAME = TableName.valueOf("wb");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
    private ArrayBlockingQueue<Put> waitingPuts = new ArrayBlockingQueue<>(10000);
    private ConcurrentHashMap<String, Object> waitingPutsStorage = new ConcurrentHashMap<>(10000);
    private ConcurrentHashMap<String, Object> waitingPutsMiniStorage = new ConcurrentHashMap<>(100);
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
        if (waitingPutsMiniStorage.containsKey(url) || waitingPutsStorage.containsKey(url))
        {
            return true;
        }

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

    public boolean[] exists(ArrayList<String> urls) throws IOException
    {
        ArrayList<Get> gets = new ArrayList<>();
        boolean[] result = new boolean[urls.size()];
        for (int i = 0; i < result.length; i++)
        {
            result[i] = false;
            Get get = new Get(Bytes.toBytes(urls.get(i)));
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("bodyText"));
            gets.add(get);
        }

        updateExistsResult(waitingPutsStorage, urls, result);
        updateExistsResult(waitingPutsMiniStorage, urls, result);

        Table table = null;

        try
        {
            table = hbaseConnection.getTable(TABLE_NAME);
            boolean[] mainStorageExistResult = table.existsAll(gets);
            updateExistsResult(mainStorageExistResult, result);
        } finally
        {
            if (table != null)
                table.close();
        }

        return result;
    }

    private void updateExistsResult(boolean[] mainStorageExistResult, boolean[] result)
    {
        for (int i = 0; i < result.length; i++)
        {
            if (result[i])
            {
                continue;
            }

            result[i] = mainStorageExistResult[i];
        }
    }

    private void updateExistsResult(ConcurrentHashMap<String, Object> waitingPutsStorage, ArrayList<String> urls, boolean[] result)
    {
        for (int i = 0; i < result.length; i++)
        {
            if (result[i])
            {
                continue;
            }

            if (waitingPutsStorage.containsKey(urls.get(i)))
            {
                result[i] = true;
            }
        }
    }

    public void put(PageInfo pageInfo) throws IOException
    {
        if (pageInfo.getUrl().equals("") || pageInfo.getUrl() == null)
        {
            return; //TODO good?
        }

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

            waitingPuts.put(put);
            waitingPutsStorage.put(new String(put.getRow()), new Object());
        } catch (InterruptedException e)
        {
            e.printStackTrace(); //TODO
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
//                t2 = System.currentTimeMillis() - t2;
//                logger.info("Started adding Put classes in a list after " + t2 + " milli seconds");

                ArrayList<Put> puts = new ArrayList<>();
                for (int i = 0; i < 100; i++)
                {
                    try
                    {
                        Put put = waitingPuts.take();
                        puts.add(put);
                        waitingPutsMiniStorage.put(new String(put.getRow()), new Object());
                        waitingPutsStorage.remove(new String(put.getRow()));
                    } catch (InterruptedException e)
                    {
                        //TODO what to do?!
                    }
                }

                Table table = null;

                try
                {
//                    t1 = System.currentTimeMillis();
//                    t1 = System.currentTimeMillis() - t1;
//                    logger.info("100 put done in " + t1 + " milli seconds");
//                    t2 = System.currentTimeMillis();

                    table = hbaseConnection.getTable(TABLE_NAME);
                    table.put(puts);
//                    Profiler.putDone(100);
                    waitingPutsMiniStorage.clear();
                } catch (IOException e) //TODO IllegalArgumentException
                {
                    logger.error("Failed to put in hbase"); //TODO
                } finally
                {
                    if (table != null)
                    {

                        try
                        {
                            table.close();
                        } catch (IOException e)
                        {
                            logger.error("Failed to close the table"); //TODO
                        }
                    }
                }
            }
        }).start();
    }
}
