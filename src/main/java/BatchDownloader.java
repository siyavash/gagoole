import javafx.util.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import util.Profiler;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;

class BatchDownloader extends Thread {
    private CloseableHttpAsyncClient client;
    private HttpContext context;
    private HttpGet request;
    private ArrayBlockingQueue<Pair<String, String>> downloadedData;
    String url;

    public BatchDownloader(CloseableHttpAsyncClient client, HttpGet req, ArrayBlockingQueue<Pair<String, String>> downloadedData, String url){
        this.client = client;
        context = HttpClientContext.create();
        this.request = req;
        this.downloadedData = downloadedData;
        this.url = url;
    }

    @Override
    public void run() {
        try {
            Future<HttpResponse> future = client.execute(request, context, null);
            HttpResponse response = future.get();
            if(response.getStatusLine().getStatusCode() == 200) {
                downloadedData.put(new Pair<>(EntityUtils.toString(response.getEntity()), url));
                Profiler.downloadDone();
            }
        } catch (Exception ex) {
            System.out.println(ex.getLocalizedMessage());
        }
    }
}