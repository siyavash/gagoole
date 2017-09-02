import datastore.DataStore;
import util.Profiler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CheckNewUrl extends Thread {

    private DataStore urlDatabase;
    private ArrayBlockingQueue<String> properUrls;
    private ArrayBlockingQueue<String> newUrls;
    private final int CTHREADS;

    public CheckNewUrl(DataStore urlDatabase, ArrayBlockingQueue<String> properUrls, ArrayBlockingQueue<String> newUrls){
        this.urlDatabase = urlDatabase;
        this.properUrls = properUrls;
        this.newUrls = newUrls;
        CTHREADS = readProperty();
    }

    private int readProperty() {
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
        return Integer.parseInt(prop.getProperty("check-exist-threads-number", "8"));
    }

    public void startCheckingThreads() {
        ExecutorService checkingPool = Executors.newFixedThreadPool(CTHREADS);
        for (int i = 0; i < CTHREADS; i++) {
            checkingPool.submit((Runnable) () -> {
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
            });
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
}