package datastore;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class PageInfoDataStore implements DataStore
{
    private Connection hbaseConnection;
    private static final TableName TABLE_NAME = TableName.valueOf("wb");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");

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
        Table table = null;
        try {
            String subLinks = turnSubLinksToString(pageInfo.getSubLinks());

            byte[] urlBytes = Bytes.toBytes(pageInfo.getUrl());
            Put put = new Put(urlBytes);

            table = hbaseConnection.getTable(TABLE_NAME);

            addColumnToPut(put, Bytes.toBytes("authorMeta"), pageInfo.getAuthorMeta());
            addColumnToPut(put, Bytes.toBytes("descriptionMeta"), pageInfo.getDescriptionMeta());
            addColumnToPut(put, Bytes.toBytes("titleMeta"), pageInfo.getTitleMeta());
            addColumnToPut(put, Bytes.toBytes("contentTypeMeta"), pageInfo.getContentTypeMeta());
            addColumnToPut(put, Bytes.toBytes("keyWordsMeta"), pageInfo.getKeyWordsMeta());
            addColumnToPut(put, Bytes.toBytes("bodyText"), pageInfo.getBodyText());
            addColumnToPut(put, Bytes.toBytes("title"), pageInfo.getTitle());
            addColumnToPut(put, Bytes.toBytes("subLinks"), subLinks);

            table.put(put);
        } finally {
            if (table != null)
                table.close();
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

    public Iterator<PageInfo> getRowIterator() throws IOException
    {
        Table table = hbaseConnection.getTable(TABLE_NAME);
        ResultScanner rowScanner = table.getScanner(new Scan());
        // set caching
        return new RowIterator(rowScanner);
    }

    private PageInfo createPageInfo(Result result)
    {
        PageInfo pageInfo = new PageInfo();

        pageInfo.setUrl(Arrays.toString(result.getRow()));
        pageInfo.setBodyText(toPageInfoString(result.getValue(COLUMN_FAMILY, Bytes.toBytes("bodyText"))));
        pageInfo.setTitle(toPageInfoString(result.getValue(COLUMN_FAMILY, Bytes.toBytes("title"))));
        //TODO set meta ffs
        pageInfo.setSubLinks(extractSubLinks(result));

        return pageInfo;
    }

    private String toPageInfoString(byte[] bodyTexts)
    {
        if (bodyTexts == null)
        {
            return null;
        }
        return Arrays.toString(bodyTexts);
    }

    private ArrayList<Pair<String, String>> extractSubLinks(Result result)
    {
        ArrayList<Pair<String, String>> subLinkPairs = new ArrayList<>();

        String storedSubLinks = Arrays.toString(result.getValue(COLUMN_FAMILY, Bytes.toBytes("subLinks")));
        for (String subLink : storedSubLinks.split("\n"))
        {
            Pair<String, String> subLinkPair = extractSubLinkPair(subLink);
            subLinkPairs.add(subLinkPair);
        }

        return subLinkPairs;
    }

    private Pair<String, String> extractSubLinkPair(String subLink)
    {
        String [] subLinkParts = subLink.split(" , ");
        String url = subLinkParts[0];
        String anchor = subLinkParts[1];

        return new Pair<>(url, anchor);
    }

    private class RowIterator implements Iterator<PageInfo>
    {
        private ResultScanner rowScanner;

        private RowIterator(ResultScanner rowScanner)
        {
            this.rowScanner = rowScanner;
        }


        @Override
        public boolean hasNext()
        {
            try
            {
                return rowScanner.next() != null;
            } catch (IOException e)
            {
                return true;
            }
        }

        @Override
        public PageInfo next()
        {
            Result nextResult;

            try
            {
                nextResult = rowScanner.next();
            } catch (IOException e)
            {
                return null;
            }

            return createPageInfo(nextResult);
        }
    }
}
