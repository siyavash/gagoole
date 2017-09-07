import com.squareup.okhttp.Call;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import javafx.util.Pair;
import util.Profiler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HtmlCollector
{
    private ArrayBlockingQueue<String> newUrls;
    private ArrayBlockingQueue<Pair<String, String>> downloadedData;
    private OkHttpClient client;
    private final int THREAD_NUMBER;


    public HtmlCollector(ArrayBlockingQueue<String> newUrls, ArrayBlockingQueue<Pair<String, String>> downloadedData/*, URLQueue allUrlQueue*/)
    {
        this.downloadedData = downloadedData;
        this.newUrls = newUrls;
        THREAD_NUMBER = readProperty();
        createAndConfigClient();
    }

    private int readProperty()
    {
        Properties prop = new Properties();

        try (InputStream input = new FileInputStream("config.properties"))
        {
            prop.load(input);
        } catch (IOException ex)
        {
            Profiler.error("Error while reading config file");
        }

        return Integer.parseInt(prop.getProperty("download-html-threads-number", "200"));
    }

    private void createAndConfigClient()
    {
        client = new OkHttpClient();
        client.setReadTimeout(1500, TimeUnit.MILLISECONDS);
        client.setConnectTimeout(1500, TimeUnit.MILLISECONDS);
        //client.setFollowRedirects(false);                     //it should be removed
        client.setFollowSslRedirects(false);
        client.setRetryOnConnectionFailure(false);
    }

    public void startDownloadThreads()
    {
        if (THREAD_NUMBER == 0)
        {
            return;
        }

        ExecutorService downloadPool = Executors.newFixedThreadPool(THREAD_NUMBER);

        for (int i = 0; i < THREAD_NUMBER; i++)
        {
            downloadPool.submit((Runnable) () -> {
                TimeoutThread timeoutThread = new TimeoutThread();
                timeoutThread.start();
                while (true)
                {
                    try
                    {
                        String htmlBody;
                        String url = newUrls.take();

                        htmlBody = getPureHtmlFromLink(url, timeoutThread);

                        if (htmlBody != null)
                        {
                            putUrlBody(htmlBody, url);
                        }

                    } catch (InterruptedException ignored)
                    {

                    }
                }
            });
        }
        downloadPool.shutdown();
    }


    private void putUrlBody(String urlHtml, String url) throws InterruptedException
    {
        Pair<String, String> dataPair = new Pair<>(urlHtml, url);
        downloadedData.put(dataPair);

        if (dataPair.getKey() != null)
        {
            Profiler.downloadDone();
        }

    }

    private String getPureHtmlFromLink(String url, TimeoutThread timeoutThread)
    {
        Request request = new Request.Builder().url(url).build();
        Response response = null;
        String body = null;
        try
        {
            Call call = client.newCall(request);
            timeoutThread.addCall(call, System.currentTimeMillis());
            response = call.execute();

            if (!response.isSuccessful())
            {
                throw new IOException();
            }

            body = response.body().string();
            response.body().close();
        } catch (IOException e)
        {
            Profiler.downloadFailed();
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