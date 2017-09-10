import com.squareup.okhttp.Call;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import javafx.util.Pair;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.http.*;
import org.apache.http.auth.AuthOption;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.MalformedChallengeException;
import org.apache.http.client.AuthenticationStrategy;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.UserTokenHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.conn.ssl.BrowserCompatHostnameVerifier;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.NHttpClientEventHandler;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.nio.client.HttpPipeliningClient;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import util.Profiler;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.*;

public class HtmlCollector
{
    private ArrayBlockingQueue<String> newUrls;
    private ArrayBlockingQueue<Pair<String, String>> downloadedData;
    //    private OkHttpClient client;
//    private CloseableHttpAsyncClient client;
//    private Semaphore semaphore = new Semaphore(2000);
    private final int THREAD_NUMBER;


    public HtmlCollector(ArrayBlockingQueue<String> newUrls, ArrayBlockingQueue<Pair<String, String>> downloadedData/*, URLQueue allUrlQueue*/)
    {
        this.downloadedData = downloadedData;
        this.newUrls = newUrls;
        THREAD_NUMBER = readProperty();
//        createAndConfigClient();
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

    private CloseableHttpAsyncClient createAndConfigClient()
    {
//        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(10000).setConnectTimeout(10000)
//                .setSocketTimeout(10000)
//                .build();
//        ConnectionReuseStrategy connectionReuseStrategy = (response, context) -> false;
//
//        ConnectionConfig connectionConfig = ConnectionConfig.custom().setBufferSize(4 * 1024).build();
//        ConnectingIOReactor connectingIOReactor = null;
//        try
//        {
//            connectingIOReactor = new DefaultConnectingIOReactor();
//        } catch (IOReactorException e)
//        {
//            e.printStackTrace();
//        }
//        PoolingNHttpClientConnectionManager poolingNHttpClientConnectionManager = new PoolingNHttpClientConnectionManager(connectingIOReactor);
//        poolingNHttpClientConnectionManager.setDefaultConnectionConfig(connectionConfig);
//        poolingNHttpClientConnectionManager.setDefaultMaxPerRoute(1000);
//        poolingNHttpClientConnectionManager.setMaxTotal(1000);
//
//        IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setConnectTimeout(10000).setIoThreadCount(50).setSoKeepAlive(false).setSoReuseAddress(false)
//                .setSoTimeout(10000).build();
//
//        CloseableHttpAsyncClient client = HttpAsyncClientBuilder.create().disableAuthCaching().disableConnectionState().disableCookieManagement()
//                .setConnectionReuseStrategy(connectionReuseStrategy).setDefaultConnectionConfig(connectionConfig)
//                .setDefaultIOReactorConfig(ioReactorConfig).setMaxConnPerRoute(2000).setMaxConnTotal(2000).setDefaultRequestConfig(requestConfig)
//                .setConnectionManager(poolingNHttpClientConnectionManager).build();

//        try
//        {
//            IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
//                    .setIoThreadCount(Runtime.getRuntime().availableProcessors())
//                    .setConnectTimeout(30_000)
//                    .setSoTimeout(30_000)
//                    .build();
//
//            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
//
//
////            Registry<SchemeIOSessionStrategy> sessionStrategyRegistry = RegistryBuilder.<SchemeIOSessionStrategy>create()
////                    .register("http", NoopIOSessionStrategy.INSTANCE)
////                    .register("https", )
////                    .build();
//
//        } catch (Exception e)
//        {
//
//        }
//        client = HttpAsyncClientBuilder.create().setMaxConnPerRoute(20).setMaxConnTotal(1000).build();
//
//
//        return client;
        return HttpAsyncClientBuilder.create().setMaxConnTotal(1000).setMaxConnPerRoute(20).build();

//        client = HttpAsyncClientBuilder.create().setDefaultRequestConfig(requestConfig).build(); //TODO check other configs

//        client.start();

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
                CloseableHttpAsyncClient client = createAndConfigClient();
                client.start();
//                Semaphore semaphore = new Semaphore(2);
                while (true)
                {
                    try
                    {
                        String htmlBody;
                        ArrayList<String> urls = new ArrayList<>();

                        for (int j = 0; j < 200; j++)
                        {
                            urls.add(newUrls.take());
                        }
//                        String url = newUrls.take();
//
//                        getPureHtmlFromLink(url, timeoutThread);

//                        if (htmlBody != null)
//                        {
//                            putUrlBody(htmlBody, url);
//                        }
//                        getPureHtmlFromLink(url, timeoutThread, client, semaphore);
                        performRequests(urls, client);


                    } catch (InterruptedException ignored)
                    {
                        break;
                    } catch (URISyntaxException e)
                    {
                        Profiler.downloadFailed();
                        Profiler.error("Wrong URI syntax");
                    } catch (MalformedURLException e)
                    {
                        Profiler.downloadFailed();
                        Profiler.error("Error in creating URL");
                    } /*catch (ExecutionException | IOException | TimeoutException e)
                    {
                        Profiler.downloadFailed();
                        e.printStackTrace();
                    }*/ catch (Exception e)
                    {
                        Profiler.downloadFailed();
                        e.printStackTrace();
                    }
                }
            });
        }
        downloadPool.shutdown();
    }

    private void performRequests(ArrayList<String> urls, CloseableHttpAsyncClient client) throws IOException, URISyntaxException, InterruptedException, ExecutionException, TimeoutException
    {
        ArrayList<Future<HttpResponse>> futures = new ArrayList<>();
        client.start();
        for (String url : urls)
        {
            HttpGet get = new HttpGet(new URL(url).toURI());
            futures.add(client.execute(get, null));
        }

        for (int i = 0; i < futures.size(); i++)
        {
            String body = EntityUtils.toString(futures.get(i).get(10, TimeUnit.SECONDS).getEntity());
            putUrlBody(body, urls.get(i));
        }
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

//    private void getPureHtmlFromLink(String url, TimeoutThread timeoutThread) throws InterruptedException, MalformedURLException, URISyntaxException
//    {
//        HttpGet get = new HttpGet(new URL(url).toURI());
//        Future<HttpResponse> futureResponse = client.execute(get, new FutureCallback<HttpResponse>()
//        {
//            @Override
//            public void completed(HttpResponse result)
//            {
//                try
//                {
//                    String body = EntityUtils.toString(result.getEntity());
//                    if (body == null)
//                    {
//                        Profiler.error("Null body");
//                        return;
//                    }
//                    semaphore.release();
////                    get.releaseConnection();
//                    putUrlBody(body, url);
//                } catch (InterruptedException ignored)
//                {
//
//                } catch (IOException e)
//                {
//                    Profiler.error("Error while reading page body");
//                }
//            }
//
//            @Override
//            public void failed(Exception ex)
//            {
//                semaphore.release();
//                Profiler.downloadFailed();
//            }
//
//            @Override
//            public void cancelled()
//            {
//                semaphore.release();
//                Profiler.downloadCanceled();
//            }
//        });
//        semaphore.acquire();
//        timeoutThread.addResponse(get, System.currentTimeMillis());
//
////        Request request = new Request.Builder().url(url).build();
////        Response response = null;
////        String body = null;
////        try
////        {
////            Call call = client.newCall(request);
////            timeoutThread.addCall(call, System.currentTimeMillis());
////            response = call.execute();
////
////            if (!response.isSuccessful())
////            {
////                throw new IOException();
////            }
////
////            body = response.body().string();
////            response.body().close();
////        } catch (IOException e)
////        {
////            Profiler.downloadFailed();
////        } finally
////        {
////            if (response != null && response.body() != null)
////            {
////                try
////                {
////                    response.body().close();
////                } catch (IOException e)
////                {
////                    e.printStackTrace();
////                }
////            }
////        }
////
////        return body;
//    }

}