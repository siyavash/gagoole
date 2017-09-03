import queue.URLQueue;

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

//        AtomicInteger atomicInteger = new AtomicInteger(0);
//        Timer timer = new Timer();
//        timer.scheduleAtFixedRate(new TimerTask() {
//            @Override
//            public void run() {
//                System.out.println(atomicInteger.get() + " " + allUrlsQueue.size());
//                atomicInteger.set(0);
//            }
//        }, 0, 1000);

        for (int i = 0; i < THREAD_NUMBER; i++) {
            fetchingPool.submit((Runnable) () -> {
                while (true){
//                    atomicInteger.incrementAndGet();

//                    long allFetchingTasksTime = System.currentTimeMillis();
//                    long singleFetchingTaskTime;

                    //get url
//                    singleFetchingTaskTime = System.currentTimeMillis();
                    ArrayList<String> urlsToVisit = new ArrayList<>();
                    for (int j = 0; j < 200; j++)
                    {
                        urlsToVisit.add(getUrlFromQueue());
                    }
//                    String urlToVisit = getUrlFromQueue();
//                    Profiler.setQueueSize(allUrlsQueue.size());
//                    if (urlToVisit == null || urlToVisit.startsWith("ftp") || urlToVisit.startsWith("mailto"))
//                    {
//                        continue;
//                    }
//                    singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
//                    Profiler.getLinkFromKafkaQueue(urlToVisit, singleFetchingTaskTime);

                    //check politeness
//                    singleFetchingTaskTime = System.currentTimeMillis();
                    boolean[] isPolite = checkIfPolite(urlsToVisit);
                    boolean[] isGoodContentType = isGoodContentType(urlsToVisit);
//                    singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
//                    Profiler.checkPolitensess(urlToVisit, singleFetchingTaskTime, isPolite);
                    for (int j = 0; j < 200; j++) {
                        if (!isPolite[j]) {
                            allUrlsQueue.push(urlsToVisit.get(j));
                            continue;
                        }
                        if (!isGoodContentType[j])
                            continue;
                        addUrlToProperUrls(urlsToVisit.get(j));
                    }


                    //check content type
//                    singleFetchingTaskTime = System.currentTimeMillis();

//                    singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
//                    Profiler.checkContentType(urlToVisit, singleFetchingTaskTime, isGoodContentType);


                    //finish
//                    singleFetchingTaskTime = System.currentTimeMillis();

//                    singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
//                    Profiler.pushUrlToProperQueue(urlToVisit, singleFetchingTaskTime);

//                    Profiler.setPropersSize(properUrls.size());
//                    allFetchingTasksTime = System.currentTimeMillis() - allFetchingTasksTime;
//                    Profiler.getLinkFinished(urlToVisit, allFetchingTasksTime);

//                    atomicInteger.incrementAndGet();
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

    private boolean[] checkIfPolite(ArrayList<String> urlsToVisit) {
        int n = urlsToVisit.size();
        boolean [] isPolite = new boolean[n];
        int i = 0;
        for (String urlToVisit:urlsToVisit) {
            int index = urlToVisit.indexOf("/", 8);
            if (index == -1) index = urlToVisit.length();
            isPolite[i] = !cache.checkIfExist(urlToVisit.substring(0, index));
            i++;
        }
        return isPolite;
    }

    private boolean[] isGoodContentType(ArrayList<String> urls) {
        int n = urls.size();
        boolean [] isGood = new boolean[n];
        int i = 0;
        for (String url:urls){
            url = url.toLowerCase();
            isGood[i] = !url.endsWith(".jpg") && !url.endsWith(".gif") && !url.endsWith(".pdf") && !url.endsWith(".deb")
                    && !url.endsWith(".jpeg") && !url.endsWith(".png") && !url.endsWith(".txt") && !url.endsWith(".exe")
                    && !url.endsWith(".gz") && !url.endsWith(".rar") && !url.endsWith(".zip") && !url.endsWith(".tar.gz");
            i++;
        }
        return isGood;
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