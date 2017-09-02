import queue.URLQueue;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class FetchProperUrl {

    private URLQueue allUrlsQueue;
    private final LruCache cache = new LruCache();
    private ArrayBlockingQueue<String> properUrls;
    private final int THREAD_NUMBER;

    //constructor
    public FetchProperUrl(URLQueue allUrlsQueue, ArrayBlockingQueue<String> properUrls) {
        this.allUrlsQueue = allUrlsQueue;
        this.properUrls = properUrls;
        THREAD_NUMBER = readProperty();
    }

    private int readProperty() {
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
        return Integer.parseInt(prop.getProperty("fetch-url-threads-number", "8"));
    }

    public void startFetchingThreads() {
        if (THREAD_NUMBER == 0)
        {
            return;
        }

        ExecutorService fetchingPool = Executors.newFixedThreadPool(THREAD_NUMBER);
        AtomicInteger atomicInteger = new AtomicInteger(0);
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                System.out.println(atomicInteger.get());
                atomicInteger.set(0);
            }
        }, 0, 1000);

        for (int i = 0; i < THREAD_NUMBER; i++) {
            fetchingPool.submit((Runnable) () -> {
                while (true){

//                    long allFetchingTasksTime = System.currentTimeMillis();
//                    long singleFetchingTaskTime;

                    //get url
//                    singleFetchingTaskTime = System.currentTimeMillis();
                    String urlToVisit = getUrlFromQueue();
//                    Profiler.setQueueSize(allUrlsQueue.size());
                    if (urlToVisit == null || urlToVisit.startsWith("ftp") || urlToVisit.startsWith("mailto"))
                    {
                        continue;
                    }
//                    singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
//                    Profiler.getLinkFromKafkaQueue(urlToVisit, singleFetchingTaskTime);

                    //check politeness
//                    singleFetchingTaskTime = System.currentTimeMillis();
                    boolean isPolite = checkIfPolite(urlToVisit);
//                    singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
//                    Profiler.checkPolitensess(urlToVisit, singleFetchingTaskTime, isPolite);
                    if (!isPolite) {
                        allUrlsQueue.push(urlToVisit);
                        continue;
                    }

                    //check content type
//                    singleFetchingTaskTime = System.currentTimeMillis();
                    boolean isGoodContentType = isGoodContentType(urlToVisit);
//                    singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
//                    Profiler.checkContentType(urlToVisit, singleFetchingTaskTime, isGoodContentType);
                    if (!isGoodContentType)
                        continue;

                    //finish
//                    singleFetchingTaskTime = System.currentTimeMillis();

                    addUrlToProperUrls(urlToVisit);

//                    singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
//                    Profiler.pushUrlToProperQueue(urlToVisit, singleFetchingTaskTime);

//                    Profiler.setPropersSize(properUrls.size());
//                    allFetchingTasksTime = System.currentTimeMillis() - allFetchingTasksTime;
//                    Profiler.getLinkFinished(urlToVisit, allFetchingTasksTime);

                    atomicInteger.incrementAndGet();
                }
            });
        }
        fetchingPool.shutdown();
    }

    private String getUrlFromQueue(){
        String candidateUrl = null;
        try {
            candidateUrl = allUrlsQueue.pop();
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
        return candidateUrl;
    }

    private boolean checkIfPolite(String urlToVisit) {
        int index = urlToVisit.indexOf("/", 8);
        if (index == -1) index = urlToVisit.length();
        return !cache.checkIfExist(urlToVisit.substring(0, index));
    }

    private boolean isGoodContentType(String url) {
        url = url.toLowerCase();
        return !url.endsWith(".jpg") && !url.endsWith(".gif") && !url.endsWith(".pdf") && !url.endsWith(".deb")
                && !url.endsWith(".jpeg") && !url.endsWith(".png") && !url.endsWith(".txt") && !url.endsWith(".exe")
                && !url.endsWith(".gz") && !url.endsWith(".rar") && !url.endsWith(".zip") && !url.endsWith(".tar.gz");
    }

    private void addUrlToProperUrls(String urlToVisit) {
        try {
            properUrls.put(urlToVisit);
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
    }
}