import kafka.KafkaSubscribe;

import java.io.IOException;
import java.util.ArrayList;

class Crawler {

    private int NTHREADS;

    private final KafkaSubscribe kafkaSubscribe;
    private final LruCache lruCache = new LruCache();
    private final GagooleHBase hbase;

    public Crawler(KafkaSubscribe kafkaSubscribe) throws IOException {
        this.kafkaSubscribe = kafkaSubscribe;
        hbase = new GagooleHBase("smallTable", "columnTable");
    }

    public void start() {
        ArrayList<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < NTHREADS; i++) {
            Thread thread = new CrawlExecutor(kafkaSubscribe, lruCache, hbase);
            thread.start();
            threads.add(thread);
        }
    }

    public void setThreads(int nThreads) {
        NTHREADS = nThreads;
    }

}
