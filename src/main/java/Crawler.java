import queue.BlockingQueue;
import queue.DistributedQueue;
import queue.URLQueue;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

class Crawler {

    private int NTHREADS;

    private final URLQueue queue;
    private final LruCache lruCache = new LruCache();
    private final PageInfoDataStore hbase;
    private boolean initialMode = true;
    private boolean localMode = true;
    private String bootstrapServer;
    private String topicName;
    private String zookeeperClientPort;
    private String zookeeperQuorum;

    public Crawler() {
        loadProperties();
        // TODO: initialze all elements and make all connections

        if (localMode) {
            queue = new BlockingQueue();
        } else {
            queue = new DistributedQueue(bootstrapServer, topicName);
        }

//        hbase = new PageInfoDataStore("2181", "master,slave");
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
            bootstrapServer = prop.getProperty("bootstrap-server", "master:9092, slave:9092");
            topicName = prop.getProperty("topic-name", "test");
            zookeeperClientPort = prop.getProperty("zookeeper-client-port", "2181");
            zookeeperQuorum = prop.getProperty("zookeeper-quorum", "master,slave");
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

}
