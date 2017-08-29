import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import datastore.DataStore;
import queue.URLQueue;
import util.Profiler;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;


public class LinkFilterThread extends Thread {
    private URLQueue urlArrayBlockingQueue;
    private DataStore urlDatabase;
    private OkHttpClient client;
    private final LruCache cache = new LruCache();
    private ArrayBlockingQueue<String> notYetDownloadedLinks;
    //constructor
    public LinkFilterThread(URLQueue urlArrayBlockingQueue, DataStore urlDatabase, OkHttpClient client, ArrayBlockingQueue notYetDownloadedLinks) {
        this.urlArrayBlockingQueue = urlArrayBlockingQueue;
        this.urlDatabase = urlDatabase;
        this.client = client;
        this.notYetDownloadedLinks = notYetDownloadedLinks;
    }
    private String getLinkFromQueue(){
        String candidateLink = null;
        try {
            candidateLink = urlArrayBlockingQueue.pop();
        } catch (InterruptedException e) {
            //TODO: catch deciding
        }
        return candidateLink;
    }
    private boolean checkIfPolite(String linkToVisit) {
        int index = linkToVisit.indexOf("/", 8);
        if (index == -1) index = linkToVisit.length();
        return !cache.checkIfExist(linkToVisit.substring(0, index));
    }
    private boolean checkIfAlreadyExist(String linkToVisit) {
        boolean exist = true;
        try {
            exist = urlDatabase.exists(linkToVisit);
        } catch (IOException e) {
            //TODO: catch deciding
        }
        return exist;
    }
    private boolean isGoodContentType(String link) {
        link = link.toLowerCase();
        if (link.endsWith(".jpg") || link.endsWith(".gif") || link.endsWith(".pdf") || link.endsWith(".deb")
                || link.endsWith(".jpeg") || link.endsWith(".png") || link.endsWith(".txt") || link.endsWith(".exe")
                    || link.endsWith(".gz") || link.endsWith(".rar") || link.endsWith(".zip") || link.endsWith(".tar.gz"))
            return false;
        return true;
    }

    @Override
    public void run() {
        while (true){
            long t0, timeDifference;
            //get url
            t0 = System.currentTimeMillis();
            String linkToVisit = getLinkFromQueue();
            if (linkToVisit == null || linkToVisit.startsWith("ftp") || linkToVisit.startsWith("mailto"))
                continue;
            timeDifference = System.currentTimeMillis() - t0;
            Profiler.getLinkFromQueueToCrawl(linkToVisit, timeDifference);
            //check politeness
            t0 = System.currentTimeMillis();
            boolean isPolite = checkIfPolite(linkToVisit);
            timeDifference = System.currentTimeMillis() - t0;
            Profiler.checkPolitensess(linkToVisit, timeDifference, isPolite);
            if (!isPolite) {
                Profiler.isImpolite();
                urlArrayBlockingQueue.push(linkToVisit);
                continue;
            }
            //check exist
            t0 = System.currentTimeMillis();
            boolean isInDataStore = checkIfAlreadyExist(linkToVisit);
            timeDifference = System.currentTimeMillis() - t0;
            Profiler.checkExistenceInDataStore(linkToVisit, timeDifference, isInDataStore);
            if (isInDataStore)
                continue;
            //check content type
            t0 = System.currentTimeMillis();
            boolean isGoodContentType = isGoodContentType(linkToVisit);
            timeDifference = System.currentTimeMillis() - t0;
            Profiler.checkContentType(linkToVisit, timeDifference, isGoodContentType);
            if (!isGoodContentType)
                continue;
            try {
                notYetDownloadedLinks.put(linkToVisit);
            } catch (InterruptedException e) {
                //TODO catch deciding
            }
        }
    }
}
