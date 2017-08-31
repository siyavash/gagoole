import datastore.DataStore;
import util.Profiler;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CheckNewUrl extends Thread {

    private DataStore urlDatabase;
    private ArrayBlockingQueue<String> properUrls;
    private ArrayBlockingQueue<String> newUrls;
    private final int CTHREADS;

    public CheckNewUrl(DataStore urlDatabase, ArrayBlockingQueue<String> properUrls, int checkThreadNumber){
        this.urlDatabase = urlDatabase;
        this.properUrls = properUrls;
        newUrls = new ArrayBlockingQueue<>(100000);
        CTHREADS = checkThreadNumber;
        startCheckingThreads();
    }

    private void startCheckingThreads() {
        ExecutorService checkingPool = Executors.newFixedThreadPool(CTHREADS);
        for (int i = 0; i < CTHREADS; i++) {
            checkingPool.submit(this);
        }
        checkingPool.shutdown();
        try {
            checkingPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            //TODO exception handling
        }
    }

    private String getProperUrl() {
        String urlToVisit = null;
        try {
            urlToVisit = properUrls.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
        return urlToVisit;
    }

    private boolean checkIfAlreadyExist(String urlToVisit) {   //I/O work
        boolean exist = true;
        try {
            exist = urlDatabase.exists(urlToVisit);
        } catch (IOException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
        return exist;
    }

    private void putNewUrl(String urlToVisit) {
        try {
            newUrls.put(urlToVisit);
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
    }

    @Override
    public void run() {
        while(true){
            long allCheckingTime = System.currentTimeMillis();
            String urlToVisit = getProperUrl();
            if (urlToVisit == null)
                continue;
            boolean isInDataStore = checkIfAlreadyExist(urlToVisit);
            allCheckingTime = System.currentTimeMillis() - allCheckingTime;
            Profiler.checkExistenceInDataStore(urlToVisit, allCheckingTime, isInDataStore);
            if (isInDataStore)
                continue;
            putNewUrl(urlToVisit);
        }

    }
}
