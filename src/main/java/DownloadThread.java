import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import javafx.util.Pair;
import util.Profiler;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class DownloadThread extends Thread
{
    private ArrayBlockingQueue<String> notYetDownloadedLinksBlockingQueue;
    private ArrayBlockingQueue<Pair<String, String>> downloadedDataBlockingQueue;
    private OkHttpClient client;

    public DownloadThread(ArrayBlockingQueue<String> notYetDownloadedLinksBlockingQueue, ArrayBlockingQueue<Pair<String, String>> downloadedDataBlockingQueue)
    {
        this.downloadedDataBlockingQueue = downloadedDataBlockingQueue;
        this.notYetDownloadedLinksBlockingQueue = notYetDownloadedLinksBlockingQueue;
    }

    @Override
    public void run()
    {
        client = new OkHttpClient();
        while (true)
        {
            long t1 = System.currentTimeMillis();
            String downloadedData;
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

                if(!isHtml(downloadedData))
                    continue;
            } catch (IOException | IllegalArgumentException e)
            {
                continue;
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
        return downloadedData.contains("<html>") && downloadedData.contains("</html>");
    }

    private String getPureHtmlFromLink(String link) throws IOException
    {
        if (link == null)
        {
            return null;
        }
        Request request = new Request.Builder().url(link).build();
        Response response = client.newCall(request).execute();
        return response.body().string();
    }
}
