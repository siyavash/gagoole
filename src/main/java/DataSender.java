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

public class DataSender
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

        Properties prop = new Properties();
        InputStream input = null;

        try
        {
            input = new FileInputStream("config.properties");
            prop.load(input);
        } catch (IOException ex)
        {
            System.err.println("error in reading config file:");
            ex.printStackTrace();
        } finally
        {
            if (input != null)
            {
                try
                {
                    input.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        THREAD_NUMBER = Integer.parseInt(prop.getProperty("sender-thread-number", "100"));
    }

    public void startSending()
    {
        if (THREAD_NUMBER == 0)
        {
            return;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUMBER);
//        AtomicInteger atomicInteger = new AtomicInteger(0);
//        Timer timer = new Timer();
//        timer.scheduleAtFixedRate(new TimerTask() {
//            @Override
//            public void run() {
//                System.out.println("Data sender: " + atomicInteger.get());
//                atomicInteger.set(0);
//            }
//        }, 0, 1000);
        for (int i = 0; i < THREAD_NUMBER; i++)
        {
            executorService.submit(() -> {
                while (true)
                {
                    try
                    {
                        PageInfo pageInfo = popNewPageInfo();
                        sendToDataStore(pageInfo);
                        Profiler.putDone(1);
                        pushSubLinksToQueue(pageInfo);
                    } catch (InterruptedException ignored)
                    {

                    }
//                    atomicInteger.incrementAndGet();
                }
            });
        }
        executorService.shutdown();
    }

    private void pushSubLinksToQueue(PageInfo pageInfo)
    {
//        long time = System.currentTimeMillis();
        ArrayList<String> subLinks = getAllSubLinksFromPageInfo(pageInfo);

        urlQueue.push(subLinks);

//        time = System.currentTimeMillis() - time;
//        Profiler.pushToQueue(pageInfo.getUrl(), time);
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

    private void sendToDataStore(PageInfo pageInfo) throws IOException
    {
//        long time = System.currentTimeMillis();

        dataStore.put(pageInfo);

//        time = System.currentTimeMillis() - time;
//        Profiler.putToDataStore(pageInfo.getUrl(), time);
    }

    private PageInfo popNewPageInfo() throws InterruptedException
    {
//        long time = System.currentTimeMillis();

        PageInfo pageInfo = organizedData.take();

//        time = System.currentTimeMillis() - time;
//        Profiler.popOrganizedData(pageInfo.getUrl(), time);

        return pageInfo;
    }
}
