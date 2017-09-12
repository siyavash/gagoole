package datastore;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class PageInfoDataStore implements DataStore
{
    private Connection hbaseConnection;
    private static final TableName TABLE_NAME = TableName.valueOf("w");
    private static final byte[] MAIN_COLUMN_FAMILY = Bytes.toBytes("cf");
    private static final byte[] SUBLINKS_COLUMN_FAMILY = Bytes.toBytes("sl");
    private ArrayBlockingQueue<Put> waitingPuts = new ArrayBlockingQueue<>(1000);
    private ConcurrentHashMap<String, Object> waitingPutsStorage = new ConcurrentHashMap<>(1000);
    private ConcurrentHashMap<String, Object> waitingPutsMiniStorage = new ConcurrentHashMap<>(20);
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
        boolean result;
        try
        {
            table = hbaseConnection.getTable(TABLE_NAME);
            Get get = new Get(Bytes.toBytes(url));
            result = table.exists(get);
        } finally
        {
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

        checkInsideStorage(waitingPutsStorage, urls, result);
        checkInsideStorage(waitingPutsMiniStorage, urls, result);
        checkMainStorage(result, gets);

        return result;
    }

    private void checkMainStorage(boolean[] result, ArrayList<Get> gets) throws IOException
    {
        boolean[] mainStorageExistResult;

        try (Table table = hbaseConnection.getTable(TABLE_NAME))
        {
            mainStorageExistResult = table.existsAll(gets);
        }

        for (int i = 0; i < result.length; i++)
        {
            if (result[i])
            {
                continue;
            }

            result[i] = mainStorageExistResult[i];
        }
    }

    private void checkInsideStorage(ConcurrentHashMap<String, Object> waitingPutsStorage, ArrayList<String> urls, boolean[] result)
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
            return;
        }

        try
        {
            String subLinks = turnSubLinksToString(pageInfo.getSubLinks());

            byte[] urlBytes = Bytes.toBytes(pageInfo.getUrl());
            Put put = new Put(urlBytes);

            addColumnToPut(put, MAIN_COLUMN_FAMILY, Bytes.toBytes("authorMeta"), pageInfo.getAuthorMeta());
            addColumnToPut(put, MAIN_COLUMN_FAMILY, Bytes.toBytes("descriptionMeta"), pageInfo.getDescriptionMeta());
            addColumnToPut(put, MAIN_COLUMN_FAMILY, Bytes.toBytes("contentTypeMeta"), pageInfo.getContentTypeMeta());
            addColumnToPut(put, MAIN_COLUMN_FAMILY, Bytes.toBytes("keyWordsMeta"), pageInfo.getKeyWordsMeta());
            addColumnToPut(put, MAIN_COLUMN_FAMILY, Bytes.toBytes("bodyText"), pageInfo.getBodyText());
            addColumnToPut(put, MAIN_COLUMN_FAMILY, Bytes.toBytes("title"), pageInfo.getTitle());
            addColumnToPut(put, SUBLINKS_COLUMN_FAMILY, Bytes.toBytes("subLinks"), subLinks);

            waitingPuts.put(put);
            waitingPutsStorage.put(new String(put.getRow()), new Object());
        } catch (InterruptedException e)
        {
            logger.error("Failed to put to ArrayBlockingQueue in DataStore");
        }
    }

    private void addColumnToPut(Put put, byte[] columnFamily, byte[] columnName, String value)
    {
        if (value == null)
        {
            return;
        }

        put.addColumn(columnFamily, columnName, Bytes.toBytes(value));
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
            stringBuilder.append("\n");
            stringBuilder.append(anchorName);
            stringBuilder.append("\n");
        }

        return stringBuilder.toString();
    }

    private void startPuttingToTable()
    {
        new Thread(() -> {
            while (true)
            {
                try
                {

                    ArrayList<Put> puts = new ArrayList<>();
                    for (int i = 0; i < 50; i++)
                    {

                        Put put = waitingPuts.take();
                        puts.add(put);
                        waitingPutsMiniStorage.put(new String(put.getRow()), new Object());
                        waitingPutsStorage.remove(new String(put.getRow()));

                    }

                    try (Table table = hbaseConnection.getTable(TABLE_NAME))
                    {

                        table.put(puts);
                        waitingPutsMiniStorage.clear();
                    } catch (IllegalArgumentIOException e)
                    {
                        logger.error("KeyValue size is too big. Change default settings");
                    } catch (IOException e)
                    {
                        logger.error("Failed to put in hbase");
                    }
                } catch (InterruptedException e)
                {
                    break;
                }

            }
        }).start();
    }
}
