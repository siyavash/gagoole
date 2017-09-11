import datastore.DataStore;
import datastore.PageInfo;
import javafx.util.Pair;
import queue.URLQueue;
import util.Profiler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class DataSender extends CrawlerPart
{
    private DataStore dataStore;
    private URLQueue urlQueue;
    private ArrayBlockingQueue<PageInfo> organizedData;
    private final int THREAD_NUMBER;

    public DataSender(DataStore dataStore, URLQueue urlQueue, ArrayBlockingQueue<PageInfo> organizedData)
    {
        this.dataStore = dataStore;
        this.urlQueue = urlQueue;
        this.organizedData = organizedData;
        THREAD_NUMBER = readProperty();
    }

    private int readProperty() {
        Properties prop = new Properties();

        try (InputStream input = new FileInputStream("config.properties"))
        {
            prop.load(input);
        } catch (IOException ex)
        {
            Profiler.error("Error while reading config file");
        }

        return Integer.parseInt(prop.getProperty("sender-thread-number", "100"));
    }

    @Override
    public void startThreads()
    {
        if (THREAD_NUMBER == 0)
        {
            return;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUMBER);

        for (int i = 0; i < THREAD_NUMBER; i++)
        {
            executorService.submit((Runnable) () -> {
                while (true)
                {
                    try
                    {
                        PageInfo pageInfo = organizedData.take();
                        dataStore.put(pageInfo);
                        pushSubLinksToQueue(pageInfo);
                        Profiler.putDone(1);
                    } catch (InterruptedException e)
                    {
                        break;
                    } catch (IOException e)
                    {
                        Profiler.error("Error in putting pageInfo to hbase");
                    }
                }
            });
        }
        executorService.shutdown();
        setExecutorService(executorService);
    }

    private void pushSubLinksToQueue(PageInfo pageInfo)
    {
        ArrayList<String> subLinks = getAllSubLinksFromPageInfo(pageInfo);
        urlQueue.push(subLinks);
    }

    private ArrayList<String> getAllSubLinksFromPageInfo(PageInfo pageInfo)
    {
        ArrayList<String> subLinks = new ArrayList<>();
        for (Pair<String, String> pair : pageInfo.getSubLinks())
        {
            subLinks.add(pair.getKey());
        }
        return subLinks;
    }
}