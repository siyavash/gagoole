import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import javafx.util.Pair;
import queue.URLQueue;
import util.Profiler;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DownloadThread extends Thread
{
    private ArrayBlockingQueue<String> notYetDownloadedLinksBlockingQueue;
    private ArrayBlockingQueue<Pair<String, String>> downloadedDataBlockingQueue;
    private URLQueue urlQueue;
    private OkHttpClient client;

    public DownloadThread(ArrayBlockingQueue<String> notYetDownloadedLinksBlockingQueue, ArrayBlockingQueue<Pair<String, String>> downloadedDataBlockingQueue, URLQueue urlQueue)
    {
        this.downloadedDataBlockingQueue = downloadedDataBlockingQueue;
        this.notYetDownloadedLinksBlockingQueue = notYetDownloadedLinksBlockingQueue;
        this.urlQueue = urlQueue;
        client = new OkHttpClient();
        client.setReadTimeout(1000, TimeUnit.MILLISECONDS);
        client.setConnectTimeout(1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run()
    {
        while (true)
        {
            long t1 = System.currentTimeMillis();
            String downloadedData = null;
            String link;

            try
            {
                link = notYetDownloadedLinksBlockingQueue.take();
            } catch (InterruptedException e)
            {
                continue; //TODO check if this is good enough
            }

            try
            {
                long time = System.currentTimeMillis();
                downloadedData = getPureHtmlFromLink(link);
                time = System.currentTimeMillis() - time;
                Profiler.download(link, time);

                time = System.currentTimeMillis();
                if(!isHtml(downloadedData))
                {
                    continue;
                }
                time = System.currentTimeMillis() - time;
                Profiler.htmlCheck(link, time);

            } catch (IOException e)
            {
                long time = System.currentTimeMillis();
                urlQueue.push(link);
                time = System.currentTimeMillis() - time;
                Profiler.pushBackToKafka(link, time);

                continue;
            } catch (IllegalArgumentException e){
                //TODO
            }

            try
            {
                downloadedDataBlockingQueue.put(new Pair<>(downloadedData, link));
            } catch (InterruptedException e)
            {
                e.printStackTrace(); //TODO
            }

            t1 = System.currentTimeMillis() - t1;
            Profiler.downloadThread(link, t1);
        }

    }

    private boolean isHtml(String downloadedData) {
        downloadedData = downloadedData.toLowerCase();
        return downloadedData.contains("<html") && downloadedData.contains("</html>");
    }

    private String getPureHtmlFromLink(String link) throws IOException
    {
        if (link == null)
        {
            return null;
        }
        Request request = new Request.Builder().url(link).build();
        Response response = client.newCall(request).execute();
        String body = response.body().string();
        response.body().close();

        return body;
    }
}
