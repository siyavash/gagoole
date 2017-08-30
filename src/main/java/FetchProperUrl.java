import datastore.DataStore;
import queue.URLQueue;
import util.Profiler;
import java.util.concurrent.ArrayBlockingQueue;

public class FetchProperUrl extends Thread {

    private URLQueue allUrlQueue;
    private final LruCache cache = new LruCache();
    private ArrayBlockingQueue<String> properUrls;

    //constructor
    public FetchProperUrl(URLQueue allUrlQueue, ArrayBlockingQueue<String> properUrls) {
        this.allUrlQueue = allUrlQueue;
        this.properUrls = properUrls;
    }

    private String getUrlFromQueue(){
        String candidateUrl = null;
        try {
            candidateUrl = allUrlQueue.pop();
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
        return candidateUrl;
    }

    private boolean checkIfPolite(String urlToVisit) {
        int index = urlToVisit.indexOf("/", 8);
        if (index == -1) index = urlToVisit.length();
        return !cache.checkIfExist(urlToVisit.substring(0, index));
    }

    private boolean isGoodContentType(String url) {
        url = url.toLowerCase();
        return !url.endsWith(".jpg") && !url.endsWith(".gif") && !url.endsWith(".pdf") && !url.endsWith(".deb")
                && !url.endsWith(".jpeg") && !url.endsWith(".png") && !url.endsWith(".txt") && !url.endsWith(".exe")
                && !url.endsWith(".gz") && !url.endsWith(".rar") && !url.endsWith(".zip") && !url.endsWith(".tar.gz");
    }

    private void addUrlToArrayBlockingQueue(String urlToVisit) {
        try {
            properUrls.put(urlToVisit);
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
    }

    @Override
    public void run() {
        while (true){

            long allFetchingTasksTime = System.currentTimeMillis();
            long singleFetchingTaskTime;

            //get url
            singleFetchingTaskTime = System.currentTimeMillis();
            Profiler.setQueueSize(allUrlQueue.size());
            String urlToVisit = getUrlFromQueue();
            if (urlToVisit == null || urlToVisit.startsWith("ftp") || urlToVisit.startsWith("mailto"))
            {
                continue;
            }
            singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
            Profiler.getLinkFromQueueToCrawl(urlToVisit, singleFetchingTaskTime);

            //check politeness
            singleFetchingTaskTime = System.currentTimeMillis();
            boolean isPolite = checkIfPolite(urlToVisit);
            singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
            Profiler.checkPolitensess(urlToVisit, singleFetchingTaskTime, isPolite);
            if (!isPolite) {
                Profiler.isImpolite();
                allUrlQueue.push(urlToVisit);
                continue;
            }

            //check content type
            singleFetchingTaskTime = System.currentTimeMillis();
            boolean isGoodContentType = isGoodContentType(urlToVisit);
            singleFetchingTaskTime = System.currentTimeMillis() - singleFetchingTaskTime;
            Profiler.checkContentType(urlToVisit, singleFetchingTaskTime, isGoodContentType);
            if (!isGoodContentType)
                continue;

            //finish
            addUrlToArrayBlockingQueue(urlToVisit);
            Profiler.setNotYetSize(properUrls.size());
            long totalTimeDiff = System.currentTimeMillis() - allFetchingTasksTime;
            Profiler.getLinkFinished(urlToVisit, totalTimeDiff);
        }
    }

}
