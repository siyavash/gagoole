import datastore.DataStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckNewUrl {

    private DataStore urlDatabase;
    private ArrayBlockingQueue<String> properUrls;
    private ArrayBlockingQueue<String> newUrls;
    private final int THREAD_NUMBER;

    public CheckNewUrl(DataStore urlDatabase, ArrayBlockingQueue<String> properUrls, ArrayBlockingQueue<String> newUrls){
        this.urlDatabase = urlDatabase;
        this.properUrls = properUrls;
        this.newUrls = newUrls;
        THREAD_NUMBER = readProperty();
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
        if (THREAD_NUMBER == 0)
        {
            return;
        }

        ExecutorService checkingPool = Executors.newFixedThreadPool(THREAD_NUMBER);

        AtomicInteger atomicInteger = new AtomicInteger(0);
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                System.out.println("new Urls after check hbase: " + atomicInteger.get() + ", " + properUrls.size());
                atomicInteger.set(0);
            }
        }, 0, 1000);

        for (int i = 0; i < THREAD_NUMBER; i++) {
            checkingPool.submit((Runnable) () -> {
                while(true){
//                    long allCheckingTime = System.currentTimeMillis();
//                    long singleCheckingTime = System.currentTimeMillis();

//                    String urlToVisit = getProperUrl();
//                    if (urlToVisit == null)
//                        continue;

//                    singleCheckingTime = System.currentTimeMillis() - singleCheckingTime;
//                    Profiler.getUrlToCheckIfNew(urlToVisit, singleCheckingTime);
//                    singleCheckingTime = System.currentTimeMillis();

//                    boolean isInDataStore = checkIfAlreadyExist(urlToVisit);

//                    singleCheckingTime = System.currentTimeMillis() - singleCheckingTime;
//                    Profiler.checkedExistance(urlToVisit, singleCheckingTime);
//                    allCheckingTime = System.currentTimeMillis() - allCheckingTime;
//                    Profiler.checkAllExistanceTaskTime(urlToVisit, allCheckingTime, isInDataStore);

//                    if (isInDataStore)
//                        continue;

//                    putNewUrl(urlToVisit);

//                    Profiler.setNewUrlsSize(newUrls.size());

                    ArrayList<String> urlsToVisit = new ArrayList<>();
                    for (int j = 0; j < 200; j++)
                    {
                        urlsToVisit.add(getProperUrl());
                    }

//                    boolean[] existInDataStore = checkIfAlreadyExist(urlsToVisit);

                    System.out.println("fail");

                    for (int j = 0; j < 200; j++)
                    {
                        if (/*!existInDataStore[j]*/true)
                        {
                            putNewUrl(urlsToVisit.get(j));
                            atomicInteger.incrementAndGet();
                        }
                    }


                }
            });
        }
        checkingPool.shutdown();

    }

    private boolean[] checkIfAlreadyExist(ArrayList<String> urlsToVisit)
    {
        boolean[] result = new boolean[0];

        try
        {
            result = urlDatabase.exists(urlsToVisit);
        } catch (IOException e)
        {
            e.printStackTrace();//
        }

        return result;
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