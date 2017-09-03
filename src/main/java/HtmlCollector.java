import com.squareup.okhttp.Call;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import javafx.util.Pair;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HtmlCollector {
    private ArrayBlockingQueue<String> newUrls;
    private ArrayBlockingQueue<Pair<String, String>> downloadedData;
//    private URLQueue allUrlQueue;
    private OkHttpClient client;
    private final int THREAD_NUMBER;

    public HtmlCollector(ArrayBlockingQueue<String> newUrls, ArrayBlockingQueue<Pair<String, String>> downloadedData/*, URLQueue allUrlQueue*/) {
        this.downloadedData = downloadedData;
        this.newUrls = newUrls;
//        this.allUrlQueue = allUrlQueue;
        THREAD_NUMBER = readProperty();
        createAndConfigClient();
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
        return Integer.parseInt(prop.getProperty("download-html-threads-number", "200"));
    }

    private void createAndConfigClient() {
        client = new OkHttpClient();
        client.setReadTimeout(1500, TimeUnit.MILLISECONDS);
        client.setConnectTimeout(1500, TimeUnit.MILLISECONDS);
        client.setFollowRedirects(false);
        client.setFollowSslRedirects(false);
        client.setRetryOnConnectionFailure(false);
    }

    public void startDownloadThreads() {
        if (THREAD_NUMBER == 0) {
            return;
        }

        ExecutorService downloadPool = Executors.newFixedThreadPool(THREAD_NUMBER);

//        AtomicInteger atomicInteger = new AtomicInteger(0);
//        Timer timer = new Timer();
//        timer.scheduleAtFixedRate(new TimerTask() {
//            @Override
//            public void run() {
//                System.out.println("Downloaded htmls: " + atomicInteger.get() + ", " + newUrls.size());
//                atomicInteger.set(0);
//            }
//        }, 0, 1000);

        for (int i = 0; i < THREAD_NUMBER; i++) {
            downloadPool.submit((Runnable) () -> {
                TimeoutThread timeoutThread = new TimeoutThread();
                timeoutThread.start();
                while (true) {
                    String url = getNewUrl();
                    String htmlBody = getPureHtmlFromLink(url, timeoutThread);
                    Profiler.downloadDone();
                    putUrlBody(htmlBody, url);
//                    atomicInteger.incrementAndGet();
                }
            });
        }
        downloadPool.shutdown();
    }

    private void putUrlBody(String urlHtml, String url) {
        try {
            downloadedData.put(new Pair<>(urlHtml, url));
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
    }

    private String getNewUrl() {
        String url = null;
        try {
            url = newUrls.take();
        } catch (InterruptedException e) {
            //TODO: catch deciding
        }
        return url;
    }

    private String getPureHtmlFromLink(String url, TimeoutThread timeoutThread) {
        Request request = new Request.Builder().url(url).build();
        Response response = null;
        String body = null;
        try {
            Call call = client.newCall(request);
            timeoutThread.addCall(call, System.currentTimeMillis());
            response = call.execute();

            body = response.body().string();
            response.body().close();
        } catch (IOException e) {
//            long singleDownloadingTaskTime = System.currentTimeMillis();
//            allUrlQueue.push(url);
//            singleDownloadingTaskTime = System.currentTimeMillis() - singleDownloadingTaskTime;
//            Profiler.pushBackToKafka(url, singleDownloadingTaskTime);
        } finally
        {
            if (response != null && response.body() != null)
            {
                try
                {
                    response.body().close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        return body;
    }
}