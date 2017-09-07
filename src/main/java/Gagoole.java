import com.squareup.okhttp.Call;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import util.Profiler;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Gagoole {
    public static void main(String[] args) {
//        Profiler.start();
//
//        Crawler crawler = new Crawler();
//        crawler.start();
        getPureHtmlFromLink();
    }

    private static void getPureHtmlFromLink()
    {
        OkHttpClient client = new OkHttpClient();
        client.setReadTimeout(1500, TimeUnit.MILLISECONDS);
        client.setConnectTimeout(1500, TimeUnit.MILLISECONDS);
        client.setFollowRedirects(false);
        client.setFollowSslRedirects(false);
        client.setRetryOnConnectionFailure(false);
        Request request = new Request.Builder().url("http://www.goal.com").build();
        Response response = null;
        String body = null;
        try
        {
            Call call = client.newCall(request);
            response = call.execute();

            System.out.println(response.isSuccessful());
            if (!response.isSuccessful())
            {
                throw new IOException();
            }
            body = response.body().string();
            response.body().close();
        } catch (IOException e)
        {
//            Profiler.downloadFailed();
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

        System.out.println("html is: " + body);
    }
}
