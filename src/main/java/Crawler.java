import datastore.DataStore;
import datastore.LocalDataStore;
import datastore.PageInfoDataStore;
import queue.BlockingQueue;
import queue.DistributedQueue;
import queue.URLQueue;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

class Crawler {

    private int NTHREADS;

    private final URLQueue queue;
    private final LruCache lruCache = new LruCache();
    private final DataStore dataStore;
    private boolean initialMode = true;
    private boolean localMode = true;

    public Crawler() {
        loadProperties();
        // TODO: initialze all elements and make all connections

        if (localMode) {
            queue = new BlockingQueue();
            dataStore = new LocalDataStore();
        } else {
            queue = new DistributedQueue();
            try {
                dataStore = new PageInfoDataStore();
            } catch (IOException e) {

            }
        }

        if (initialMode)
            publishSeeds();

//        hbase = new datastore.PageInfoDataStore("2181", "master,slave");
//        MyQueue urlQueue = new DistributedQueue();
    }

    public void start() {
        ArrayList<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < NTHREADS; i++) {
            //Thread thread = new CrawlThread(kafkaSubscribe, lruCache, hbase);
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    // TODO:
                }
            });
            thread.start();
            threads.add(thread);
        }
    }

    private void loadProperties() {
        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = new FileInputStream("config.properties");
            prop.load(input);

            NTHREADS = Integer.parseInt(prop.getProperty("threads-number", "500"));
            initialMode = prop.getProperty("initial-mode", "true").equals("true");
            localMode = prop.getProperty("local-mode", "true").equals("true");

        } catch (IOException ex) {
            System.err.println("error in reading config file:");
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void publishSeeds() {
        try {
            ArrayList<String> seedUrls = new ArrayList<String>(500);
            File file = new File("seed.txt");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                seedUrls.add("http://www." + line);
            }
            fileReader.close();
            queue.push(seedUrls);
        } catch (IOException e) {
            System.err.println("error in loading seed: " + e);
            System.exit(101);
        }
    }

}
