import com.squareup.okhttp.Call;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import javafx.util.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import util.Profiler;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class HtmlCollector
{
    private ArrayBlockingQueue<String> newUrls;
    private ArrayBlockingQueue<Pair<String, String>> downloadedData;
    //    private URLQueue allUrlQueue;
//    private OkHttpClient client;
//    private CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
//    private ArrayBlockingQueue<String> htmlBodies = new ArrayBlockingQueue(10000);
    private final int THREAD_NUMBER;



    public HtmlCollector(ArrayBlockingQueue<String> newUrls, ArrayBlockingQueue<Pair<String, String>> downloadedData/*, URLQueue allUrlQueue*/)
    {
        this.downloadedData = downloadedData;
        this.newUrls = newUrls;
        THREAD_NUMBER = readProperty();
//        createAndConfigClient();
//        httpclient.start();
    }

    private int readProperty()
    {
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

//    private void createAndConfigClient()
//    {
//        client = new OkHttpClient();
//        client.setReadTimeout(1500, TimeUnit.MILLISECONDS);
//        client.setConnectTimeout(1500, TimeUnit.MILLISECONDS);
//        client.setFollowRedirects(false);
//        client.setFollowSslRedirects(false);
//        client.setRetryOnConnectionFailure(false);
//    }

    public void startDownloadThreads()
    {
        if (THREAD_NUMBER == 0)
        {
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

        for (int i = 0; i < THREAD_NUMBER; i++)
        {
            downloadPool.submit((Runnable) () -> {
//                TimeoutThread timeoutThread = new TimeoutThread();
//                timeoutThread.start();
                while (true)
                {
                    String[] urls = new String[50];
                    for (int j = 0; j < 50; j++) {
                        urls[j] = getNewUrl();
                    }
                    getPureHtml(urls);
//                    String htmlBody = null;
//                    String url = getNewUrl();
//                    try{
//                        htmlBody = getPureHtmlFromLink(url/*, timeoutThread*/);
//                    } catch (ExecutionException e){
//                        continue;
//                    }
//                    try {
//                        htmlBody = getPureHtml(urls);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                    try {
//                        htmlBody = htmlBodies.take();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    if (htmlBody != null)
//                    {
//                        Profiler.downloadDone();
//                        System.out.println(url);
//                        putUrlBody(htmlBody, url);
//                    }
//                    atomicInteger.incrementAndGet();
                }
            });
        }
        downloadPool.shutdown();
    }

//    private String getPureHtmlFromLink(String url/*, TimeoutThread timeoutThread*/) throws ExecutionException {
//        final HttpGet request1 = new HttpGet(url);
//        Future<HttpResponse> future = httpclient.execute(request1, null);
//        HttpResponse response = null;
//        try {
//            response = future.get();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//
//// Get the response
//        BufferedReader rd = null;
//        try {
//            rd = new BufferedReader
//                    (new InputStreamReader(
//                            response.getEntity().getContent()));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        String line = "";
//        StringBuilder textView = null;
//        try {
//            while ((line = rd.readLine()) != null) {
//                textView.append(line);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return textView.toString();
//    }

    public void getPureHtml(String [] urls) {
        ConnectingIOReactor ioReactor = null;
        try {
            ioReactor = new DefaultConnectingIOReactor();
        } catch (IOReactorException e) {
            e.printStackTrace();
        }
        PoolingNHttpClientConnectionManager cm =
                new PoolingNHttpClientConnectionManager(ioReactor);
        CloseableHttpAsyncClient client =
                HttpAsyncClients.custom().setConnectionManager(cm).build();
        client.start();

        String[] toGet = urls;


        BatchDownloader[] threads = new BatchDownloader[toGet.length];
        for (int i = 0; i < threads.length; i++) {
            HttpGet request = new HttpGet(toGet[i]);
            threads[i] = new BatchDownloader(client, request, downloadedData, toGet[i]);
        }

        for (BatchDownloader thread : threads) {
            thread.start();
        }
        for (BatchDownloader thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

//    private void putUrlBody(String urlHtml, String url)
//    {
//        try
//        {
//            Pair<String, String> dataPair = new Pair<>(urlHtml, url);
//            downloadedData.put(dataPair);
//
//            if (dataPair.getKey() != null)
//            {
//                Profiler.downloadDone();
//            }
//        } catch (InterruptedException e)
//        {
//            e.printStackTrace();
//            //TODO: catch deciding
//        }
//    }

    private String getNewUrl()
    {
        String url = null;
        try
        {
            url = newUrls.take();
        } catch (InterruptedException e)
        {
            //TODO: catch deciding
        }
        return url;
    }

//    private String getPureHtmlFromLink(String url, TimeoutThread timeoutThread)
//    {
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
////            long singleDownloadingTaskTime = System.currentTimeMillis();
////            allUrlQueue.push(url);
////            singleDownloadingTaskTime = System.currentTimeMillis() - singleDownloadingTaskTime;
////            Profiler.pushBackToKafka(url, singleDownloadingTaskTime);
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
//    }

}