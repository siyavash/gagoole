import kafka.KafkaSubscribe;

import java.io.IOException;

class Crawler {

    private int NTHREADS;

    private final KafkaSubscribe kafkaSubscribe;
    private final LruCache lruCache = new LruCache();
    private final GagooleHBase hbase;

    public Crawler(KafkaSubscribe kafkaSubscribe) throws IOException {
        hbase = new GagooleHBase("smallTable", "columnFamily");
        this.kafkaSubscribe = kafkaSubscribe;
    }

    public void start() {

        for (int i = 0; i < NTHREADS; i++)
            new Thread(new CrawlExecutor(kafkaSubscribe, lruCache, hbase)).start();
    }

    public void setThreads(int nThreads) {
        NTHREADS = nThreads;
    }

}
