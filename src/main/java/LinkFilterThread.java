import datastore.DataStore;
import queue.URLQueue;
import util.Profiler;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;


public class LinkFilterThread extends Thread {
    private URLQueue urlQueue;
    private DataStore urlDatabase;
    private final LruCache cache = new LruCache();
    private ArrayBlockingQueue<String> notYetDownloadedLinks;
    //constructor
    public LinkFilterThread(URLQueue urlQueue, DataStore urlDatabase, ArrayBlockingQueue<String> notYetDownloadedLinks) {
        this.urlQueue = urlQueue;
        this.urlDatabase = urlDatabase;
        this.notYetDownloadedLinks = notYetDownloadedLinks;
    }
    private String getLinkFromQueue(){
        String candidateLink = null;
        try {
            long t0 = System.currentTimeMillis();
            candidateLink = urlQueue.pop();
            long timeDiff = System.currentTimeMillis() - t0;
            Profiler.getLinkFromQueueToCrawl(candidateLink, timeDiff);
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
        return !link.endsWith(".jpg") && !link.endsWith(".gif") && !link.endsWith(".pdf") && !link.endsWith(".deb")
                && !link.endsWith(".jpeg") && !link.endsWith(".png") && !link.endsWith(".txt") && !link.endsWith(".exe")
                && !link.endsWith(".gz") && !link.endsWith(".rar") && !link.endsWith(".zip") && !link.endsWith(".tar.gz");
    }
    private void addLinkToArrayBlockingQueue(String linkToVisit) {
        try {
            notYetDownloadedLinks.put(linkToVisit);
        } catch (InterruptedException e) {
            //TODO catch deciding
        }
    }

    @Override
    public void run() {
        while (true){
            long t0, timeDifference;
            //get url
//            t0 = System.currentTimeMillis();
            Profiler.setQueueSize(urlQueue.size());
            String linkToVisit = getLinkFromQueue();
            if (linkToVisit == null || linkToVisit.startsWith("ftp") || linkToVisit.startsWith("mailto"))
                continue;
//            timeDifference = System.currentTimeMillis() - t0;
//            Profiler.getLinkFromQueueToCrawl(linkToVisit, timeDifference);
            //check politeness
            t0 = System.currentTimeMillis();
            boolean isPolite = checkIfPolite(linkToVisit);
            timeDifference = System.currentTimeMillis() - t0;
            Profiler.checkPolitensess(linkToVisit, timeDifference, isPolite);
            if (!isPolite) {
                Profiler.isImpolite();
                urlQueue.push(linkToVisit);
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
            //finish
            addLinkToArrayBlockingQueue(linkToVisit);
            Profiler.setNotYetSize(notYetDownloadedLinks.size());
        }
    }
}
