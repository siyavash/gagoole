import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import javafx.util.Pair;
import queue.URLQueue;
import util.Profiler;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DownloadHtml extends Thread {
    private ArrayBlockingQueue<String> urlsToDownload;
    private ArrayBlockingQueue<Pair<String, String>> downloadedDataArrayBlockingQueue;
    private URLQueue allUrlQueue;
    private OkHttpClient client;

    public DownloadHtml(ArrayBlockingQueue<String> urlsToDownload, ArrayBlockingQueue<Pair<String, String>> downloadedDataArrayBlockingQueue, URLQueue allUrlQueue) {
        this.downloadedDataArrayBlockingQueue = downloadedDataArrayBlockingQueue;
        this.urlsToDownload = urlsToDownload;
        this.allUrlQueue = allUrlQueue;
        client = new OkHttpClient();
        client.setReadTimeout(1000, TimeUnit.MILLISECONDS);
        client.setConnectTimeout(1000, TimeUnit.MILLISECONDS);
        client.setFollowRedirects(false);
        client.setFollowSslRedirects(false);
        client.setRetryOnConnectionFailure(false);
    }

    private String getPureHtmlFromLink(String link) throws IOException {
        if (link == null) {
            return null;
        }
        Request request = new Request.Builder().url(link).build();
        Response response = client.newCall(request).execute();
        String body = response.body().string();
        response.body().close();

        return body;
    }

    @Override
    public void run() {
        while (true) {
            long allDownloadingTasksTime = System.currentTimeMillis();
            long singleDownloadingTaskTime;

            String downloadedData = null;
            String url;

            try {
                url = urlsToDownload.take();
            } catch (InterruptedException e) {
                continue;
                //TODO: catch deciding
            }

            try {
                singleDownloadingTaskTime = System.currentTimeMillis();
                downloadedData = getPureHtmlFromLink(url);
                singleDownloadingTaskTime = System.currentTimeMillis() - singleDownloadingTaskTime;
                Profiler.download(url, singleDownloadingTaskTime);

            } catch (IOException e) {
                singleDownloadingTaskTime = System.currentTimeMillis();
                allUrlQueue.push(url);
                singleDownloadingTaskTime = System.currentTimeMillis() - singleDownloadingTaskTime;
                Profiler.pushBackToKafka(url, singleDownloadingTaskTime);
                continue;
            } catch (IllegalArgumentException e){
                //TODO: catch deciding
            }

            try {
                downloadedDataArrayBlockingQueue.put(new Pair<>(downloadedData, url));
                Profiler.setDownloadedSize(downloadedDataArrayBlockingQueue.size());
            } catch (InterruptedException e) {
                e.printStackTrace();
                //TODO: catch deciding
            }

            allDownloadingTasksTime = System.currentTimeMillis() - allDownloadingTasksTime;
            Profiler.downloadThread(url, allDownloadingTasksTime);
        }

    }
}
