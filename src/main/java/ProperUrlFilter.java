import com.google.common.net.InternetDomainName;
import queue.URLQueue;
import util.Profiler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProperUrlFilter {

    private URLQueue allUrlsQueue;
    private final LruCache cache = new LruCache();
    private ArrayBlockingQueue<String> properUrls;
    private final int THREAD_NUMBER;

    //constructor
    public ProperUrlFilter(URLQueue allUrlsQueue, ArrayBlockingQueue<String> properUrls) {
        this.allUrlsQueue = allUrlsQueue;
        this.properUrls = properUrls;
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

        return Integer.parseInt(prop.getProperty("fetch-url-threads-number", "8"));
    }

    public void startFetchingThreads() {
        if (THREAD_NUMBER == 0)
        {
            return;
        }

        ExecutorService fetchingPool = Executors.newFixedThreadPool(THREAD_NUMBER);

        for (int i = 0; i < THREAD_NUMBER; i++) {
            fetchingPool.submit((Runnable) () -> {
                while (true){

                    try
                    {
                        Profiler.setAllUrlsSize(allUrlsQueue.size());
                        String urlToVisit = allUrlsQueue.pop();
                        Profiler.fetched();

                        if (urlToVisit == null || urlToVisit.startsWith("ftp") || urlToVisit.startsWith("mailto"))
                        {
                            continue;
                        }
                        //check politeness
                        boolean isPolite = checkIfPolite(urlToVisit);
                        if (!isPolite) {
                            allUrlsQueue.push(urlToVisit);
                            Profiler.notPolite();
                            continue;
                        }

                        Profiler.politeFound();
                        //check content type
                        boolean isGoodContentType = isGoodContentType(urlToVisit);
                        if (!isGoodContentType)
                        {
                            continue;
                        }

                        //finish
                        properUrls.put(urlToVisit);

                    } catch (InterruptedException ignored)
                    {

                    }

                }
            });
        }
        fetchingPool.shutdown();
    }

    private boolean checkIfPolite(String urlToVisit) {
        int index = urlToVisit.indexOf("/", 8);
        if (index == -1) index = urlToVisit.length();
        return !cache.checkIfExist(urlToVisit.substring(0, index));
    }

    private String getDomain(String stringUrl) {
        try {
            if (!stringUrl.startsWith("http")) { stringUrl = "http://" + stringUrl; }
            URL url = new URL(stringUrl);
            String hostName = url.getHost();
            return InternetDomainName.from(hostName).topPrivateDomain().toString();
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }
    }


    private boolean isGoodContentType(String url) {
        url = url.toLowerCase();
        return !url.endsWith(".jpg") && !url.endsWith(".gif") && !url.endsWith(".pdf") && !url.endsWith(".deb")
                && !url.endsWith(".jpeg") && !url.endsWith(".png") && !url.endsWith(".txt") && !url.endsWith(".exe")
                && !url.endsWith(".gz") && !url.endsWith(".rar") && !url.endsWith(".zip") && !url.endsWith(".tar.gz");

    }
}