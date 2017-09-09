import com.squareup.okhttp.Call;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import javafx.util.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import util.Profiler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.*;

public class HtmlCollector
{
    private ArrayBlockingQueue<String> newUrls;
    private ArrayBlockingQueue<Pair<String, String>> downloadedData;
//    private OkHttpClient client;
    private CloseableHttpAsyncClient client;
    private Semaphore semaphore = new Semaphore(2000);
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
        RequestConfig requestConfig = RequestConfig.custom()
                .build();

        client = HttpAsyncClientBuilder.create().setDefaultRequestConfig(requestConfig).build(); //TODO check other configs
        client.start();
//        client = new OkHttpClient();
//        client.setReadTimeout(5000, TimeUnit.MILLISECONDS);
//        client.setConnectTimeout(5000, TimeUnit.MILLISECONDS);
        //client.setFollowRedirects(false);                     //it should be removed
        //client.setFollowSslRedirects(false);
//        client.setRetryOnConnectionFailure(false);
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

//                        htmlBody = getPureHtmlFromLink(url, timeoutThread);
//
//                        if (htmlBody != null)
//                        {
//                            putUrlBody(htmlBody, url);
//                        }
                        getPureHtmlFromLink(url, timeoutThread);

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
        Profiler.setDownloadedSize(downloadedData.size());

        if (dataPair.getKey() != null)
        {
            Profiler.downloadDone();
        }

    }

    private void getPureHtmlFromLink(String url, TimeoutThread timeoutThread) throws InterruptedException
    {
        HttpGet get = new HttpGet(url);
        Future<HttpResponse> futureResponse = client.execute(get, new FutureCallback<HttpResponse>()
        {
            @Override
            public void completed(HttpResponse result)
            {
                try
                {
                    String body = EntityUtils.toString(result.getEntity());
                    if (body == null)
                    {
                        Profiler.error("Null body");
                        return;
                    }
                    semaphore.release();
                    get.releaseConnection();
                    putUrlBody(body, url);
                } catch (InterruptedException ignored)
                {

                } catch (IOException e)
                {
                    Profiler.error("Error while reading page body");
                }
            }

            @Override
            public void failed(Exception ex)
            {
                semaphore.release();
                Profiler.downloadFailed();
            }

            @Override
            public void cancelled()
            {
                semaphore.release();
                Profiler.downloadCanceled();
            }
        });
        semaphore.acquire();
        timeoutThread.addResponse(get, System.currentTimeMillis());

//        Request request = new Request.Builder().url(url).build();
//        Response response = null;
//        String body = null;
//        try
//        {
//            Call call = client.newCall(request);
//            timeoutThread.addCall(call, System.currentTimeMillis());
//            response = call.execute();
//
//            if (!response.isSuccessful())
//            {
//                throw new IOException();
//            }
//
//            body = response.body().string();
//            response.body().close();
//        } catch (IOException e)
//        {
//            Profiler.downloadFailed();
//        } finally
//        {
//            if (response != null && response.body() != null)
//            {
//                try
//                {
//                    response.body().close();
//                } catch (IOException e)
//                {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        return body;
    }

}