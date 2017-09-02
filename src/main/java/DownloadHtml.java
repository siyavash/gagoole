import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import javafx.util.Pair;
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

public class DownloadHtml {
    private ArrayBlockingQueue<String> newUrls;
    private ArrayBlockingQueue<Pair<String, String>> downloadedData;
    private URLQueue allUrlQueue;
    private OkHttpClient client;
    private final int DTHREADS;

    public DownloadHtml(ArrayBlockingQueue<String> newUrls, ArrayBlockingQueue<Pair<String, String>> downloadedData, URLQueue allUrlQueue) {
        this.downloadedData = downloadedData;
        this.newUrls = newUrls;
        this.allUrlQueue = allUrlQueue;
        DTHREADS = readProperty();
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
        return Integer.parseInt(prop.getProperty("download-html-threads-number", "8"));
    }

    private void createAndConfigClient() {
        client = new OkHttpClient();
        client.setReadTimeout(1000, TimeUnit.MILLISECONDS);
        client.setConnectTimeout(1000, TimeUnit.MILLISECONDS);
        client.setFollowRedirects(false);
        client.setFollowSslRedirects(false);
        client.setRetryOnConnectionFailure(false);
    }

    public void startDownloadThreads() {
        ExecutorService downloadPool = Executors.newFixedThreadPool(DTHREADS);
        for (int i = 0; i < DTHREADS; i++) {
            downloadPool.submit((Runnable) () -> {
                while (true) {
                    long allDownloadingTasksTime = System.currentTimeMillis();
                    long singleDownloadingTaskTime;

                    String url = getNewUrl();
                    if (url == null)
                        continue;
                    singleDownloadingTaskTime = System.currentTimeMillis();
                    String urlHtml = getPureHtmlFromLink(url);
                    singleDownloadingTaskTime = System.currentTimeMillis() - singleDownloadingTaskTime;
                    Profiler.download(url, singleDownloadingTaskTime);
                    if (urlHtml == null)
                        continue;
                    putUrlBody(urlHtml, url);
                    allDownloadingTasksTime = System.currentTimeMillis() - allDownloadingTasksTime;
                    Profiler.downloadThread(url, allDownloadingTasksTime);
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
        Profiler.setDownloadedSize(downloadedData.size());
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

    private String getPureHtmlFromLink(String url) {
        Request request = new Request.Builder().url(url).build();
        Response response = null;
        String body = null;
        try {
            response = client.newCall(request).execute();
            body = response.body().string();
            response.body().close();
        } catch (IOException e) {
            long singleDownloadingTaskTime = System.currentTimeMillis();
            allUrlQueue.push(url);
            singleDownloadingTaskTime = System.currentTimeMillis() - singleDownloadingTaskTime;
            Profiler.pushBackToKafka(url, singleDownloadingTaskTime);
        }

        return body;
    }
}