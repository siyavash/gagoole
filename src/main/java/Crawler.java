import java.io.IOException;
import java.util.ArrayList;

class Crawler {

    private int NTHREADS;

    private final KafkaSubscribe kafkaSubscribe;
    private final LruCache lruCache = new LruCache();
    private final PageInfoDataStore hbase;

    public Crawler() {
        // TODO: initialze all elements and make all connections
        hbase = new PageInfoDataStore("2181", "master,slave");
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

    public void setThreads(int nThreads) {
        NTHREADS = nThreads;
    }

}
