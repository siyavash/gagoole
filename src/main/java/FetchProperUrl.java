import queue.URLQueue;
import util.Profiler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FetchProperUrl extends Thread {

    private URLQueue allUrlsQueue;
    private final LruCache cache = new LruCache();
    private ArrayBlockingQueue<String> properUrls;
    private final int FTHREADS;

    //constructor
    public FetchProperUrl(URLQueue allUrlsQueue, ArrayBlockingQueue<String> properUrls) {
        this.allUrlsQueue = allUrlsQueue;
        this.properUrls = properUrls;
        FTHREADS = readProperty();
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
        ExecutorService fetchingPool = Executors.newFixedThreadPool(FTHREADS);
        for (int i = 0; i < FTHREADS; i++) {
            fetchingPool.submit((Runnable) () -> {
                while (true){
                    long allFetchingTasksTime = System.currentTimeMillis();
                    //get url
                    String urlToVisit = null;
                    try {
                        urlToVisit = getUrlFromQueue();
                        if (urlToVisit == null || urlToVisit.startsWith("ftp") || urlToVisit.startsWith("mailto")) {
                            continue;
                        }
                        //check politeness
                        boolean isPolite = checkIfPolite(urlToVisit);
                        if (!isPolite) {
                            Profiler.isImpolite();
                            allUrlsQueue.push(urlToVisit);
                            continue;
                        }
                        //check content type
                        boolean isGoodContentType = isGoodContentType(urlToVisit);
                        if (!isGoodContentType)
                            continue;
                        //finish
                        addUrlToProperUrls(urlToVisit);
                        Profiler.setNotYetSize(properUrls.size());
                        allFetchingTasksTime = System.currentTimeMillis() - allFetchingTasksTime;
                        Profiler.getLinkFinished(urlToVisit, allFetchingTasksTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        //TODO: catch deciding
                        continue;
                    }
                }
            });
        }
        fetchingPool.shutdown();
    }

    private String getUrlFromQueue() throws InterruptedException {
        long singleFetchingTaskTime = System.currentTimeMillis();
        String candidateUrl = allUrlsQueue.pop();
        Profiler.setQueueSize(allUrlsQueue.size());
        Profiler.getLinkFromQueueToCrawl(candidateUrl, singleFetchingTaskTime);
        return candidateUrl;
    }

    private boolean checkIfPolite(String urlToVisit) {
        long singleFetchingTaskTime = System.currentTimeMillis();
        boolean isPolite;
        int index = urlToVisit.indexOf("/", 8);
        if (index == -1) index = urlToVisit.length();
        isPolite = !cache.checkIfExist(urlToVisit.substring(0, index));
        singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
        Profiler.checkPolitensess(urlToVisit, singleFetchingTaskTime, isPolite);
        return isPolite;
    }

    private boolean isGoodContentType(String url) {
        long singleFetchingTaskTime = System.currentTimeMillis();
        url = url.toLowerCase();
        boolean isGoodType = !url.endsWith(".jpg") && !url.endsWith(".gif") && !url.endsWith(".pdf") && !url.endsWith(".deb")
                && !url.endsWith(".jpeg") && !url.endsWith(".png") && !url.endsWith(".txt") && !url.endsWith(".exe")
                && !url.endsWith(".gz") && !url.endsWith(".rar") && !url.endsWith(".zip") && !url.endsWith(".tar.gz");
        singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
        Profiler.checkContentType(url, singleFetchingTaskTime, isGoodType);
        return isGoodType;

    }

    private void addUrlToProperUrls(String urlToVisit) throws InterruptedException {
        properUrls.put(urlToVisit);
    }
}
