import datastore.DataStore;
import util.Profiler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CheckNewUrl {

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
        return Integer.parseInt(prop.getProperty("check-exist-threads-number", "200"));
    }

    public void startCheckingThreads() {
        ExecutorService checkingPool = Executors.newFixedThreadPool(CTHREADS);
        for (int i = 0; i < CTHREADS; i++) {
            checkingPool.submit((Runnable) () -> {
                while(true){
                    long allCheckingTime = System.currentTimeMillis();
                    long singleCheckingTime = System.currentTimeMillis();
                    String urlToVisit = getProperUrl();
                    if (urlToVisit == null)
                        continue;
                    singleCheckingTime = System.currentTimeMillis() - singleCheckingTime;
                    Profiler.getUrlToCheckIfNew(urlToVisit, singleCheckingTime);
                    singleCheckingTime = System.currentTimeMillis();
                    boolean isInDataStore = checkIfAlreadyExist(urlToVisit);
                    singleCheckingTime = System.currentTimeMillis() - singleCheckingTime;
                    Profiler.checkedExistance(urlToVisit, singleCheckingTime);
                    allCheckingTime = System.currentTimeMillis() - allCheckingTime;
                    Profiler.checkAllExistanceTaskTime(urlToVisit, allCheckingTime, isInDataStore);
                    if (isInDataStore)
                        continue;
                    putNewUrl(urlToVisit);
                    Profiler.setNewUrlsSize(newUrls.size());
                }
            });
        }
        checkingPool.shutdown();

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