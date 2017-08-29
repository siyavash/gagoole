import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import util.Profiler;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class DownloadThread extends Thread
{
    private ArrayBlockingQueue<String> notYetDownloadedLinksBlockingQueue;
    private ArrayBlockingQueue<String> downloadedDataBlockingQueue;
    private OkHttpClient client;

    public DownloadThread(ArrayBlockingQueue<String> notYetDownloadedLinksBlockingQueue, ArrayBlockingQueue<String> downloadedDataBlockingQueue, OkHttpClient client)
    {
        this.downloadedDataBlockingQueue = downloadedDataBlockingQueue;
        this.notYetDownloadedLinksBlockingQueue = notYetDownloadedLinksBlockingQueue;
        this.client = client;
    }

    @Override
    public void run()
    {
        while (true)
        {
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
                long t1 = System.currentTimeMillis();
                downloadedData = getPureHtmlFromLink(link);
                t1 = System.currentTimeMillis() - t1;
                Profiler.download(link, t1);
            } catch (IOException | IllegalArgumentException e)
            {
                continue;
            }

            try
            {
                downloadedDataBlockingQueue.put(downloadedData);
            } catch (InterruptedException e)
            {
                e.printStackTrace(); //TODO
            }
        }

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
