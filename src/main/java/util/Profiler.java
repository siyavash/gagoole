package util;

import com.codahale.metrics.*;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class Profiler {

    private static Logger logger = Logger.getLogger(Class.class.getName());
    private static MetricRegistry metrics = new MetricRegistry();

    private static Meter fetchedFromQueue = metrics.meter("fetchedFromQueue");
    private static Meter polite = metrics.meter("polite link");
    private static Meter unique = metrics.meter("unique url");
    private static Meter downloaded = metrics.meter("downloaded");
    private static Meter goodLanguage = metrics.meter("goodLanguage");
    private static Meter crawled = metrics.meter("crawled");
    private static Histogram linksSize = metrics.histogram("links size");

    private static AtomicLong queueSize = new AtomicLong(0);

    public static void start() {
        metrics.register("queue size", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return queueSize.get();
            }
        });

        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .build();

        reporter.start(1, TimeUnit.SECONDS);
    }

    public static void getLinkFromQueueToCrawl() {
        fetchedFromQueue.mark();
    }

    public static void isPolite() {
        polite.mark();
    }

    public static void isUnique() {
        unique.mark();
    }

    public static void download(String url, long size, long time) {
        downloaded.mark();
        linksSize.update(size / 1000);
        logger.info(String.format("Downloaded in time %d: %s", time, url));
    }

    public static void isGoodLanguage() {
        goodLanguage.mark();
    }

//    public synchronized static void checkExistenceInDataStore(String url, long time, boolean isExists) {
//        String existence = (isExists ? "exists": "doesn't exist");
//        if (time != 0)
//        logger.info(String.format("Check existence in data store (%s) in time %d: %s", existence, time, url));
//    }

    public static void setQueueSize(long size) {
        queueSize.set(size);
    }

    public static void crawled(String url, long time) {
        crawled.mark();
        logger.info(String.format("Completely crawled in time %d: %s", time, url));
    }

}
