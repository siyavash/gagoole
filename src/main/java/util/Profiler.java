package util;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class Profiler
{
    private static Logger logger = Logger.getLogger(Class.class.getName());

    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Meter fetchedFromQueue = metrics.meter("fetched from queue");
    private static final Meter polities = metrics.meter("polite links");
    private static final Meter goodLanguage = metrics.meter("good language");
    private static final Meter crawled = metrics.meter("crawled");
    private static final Meter goodContentType= metrics.meter("good content type");

    private static AtomicLong queueSize = new AtomicLong(0);
    private static AtomicLong notYetSize = new AtomicLong(0);
    private static AtomicLong downloadedSize = new AtomicLong(0);

    public static void start() {
        metrics.register(MetricRegistry.name("queueSize"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queueSize.get();
                    }
                });

        metrics.register(MetricRegistry.name("notYetSize"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queueSize.get();
                    }
                });

        metrics.register(MetricRegistry.name("downloadedSize"),
                new Gauge<Long>() {
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

    public static void getLinkFromQueueToCrawl(String url, long time) {
        logger.info(String.format("Got link from queue in time %d: %s", time, url));
        fetchedFromQueue.mark();
    }

    public static void checkPolitensess(String url, long time, boolean isPolite) {
        String politeness = (isPolite ? "is polite" : "not polite");
        logger.info(String.format("Checked Politeness (%s) in time %d: %s", politeness, time, url));
        if (isPolite) polities.mark();
    }

    public static void checkContentType(String url, long time, boolean isGood) {
        String goodness = (isGood ? "good" : "bad");
        logger.info(String.format("Checked content type (%s) in time %d: %s", isGood, time, url));
        if (isGood) goodContentType.mark();
    }

    public static void goodLanguage(String url, long time, boolean isEnglish) {
        String beingEnglish = (isEnglish ? "is english" : "not english");
        logger.info(String.format("Checked good language (%s) in time %d: %s", beingEnglish, time, url));
        if (isEnglish) goodLanguage.mark();
    }

    public static void extractInformationFromDocument(String url, long time) {
        logger.info(String.format("Extracted info from document in time %d: %s", time, url));
        crawled.mark();
    }

    public static void download(String url, long time) {
        logger.info(String.format("Downloaded in time %d: %s", time, url));
    }

    public static void parse(String url, long time) {
        logger.info(String.format("Parsed in time %d: %s", time, url));
    }

    public static void putToDataStore(String url, long time) {
        logger.info(String.format("Putted in data store in time %d: %s", time, url));
    }

    public static void checkExistenceInDataStore(String url, long time, boolean isExists) {
        String existence = (isExists ? "exists" : "does'nt exist");
        logger.info(String.format("Check existence in data store (%s) in time %d: %s", existence, time, url));
    }

    public static void setNotYetSize(long size) {
        notYetSize.set(size);
    }

    public static void setQueueSize(long size) {
        queueSize.set(size);
    }

    public static void setDownloadedSize(long size) {
        downloadedSize.set(size);
    }

    public static void crawled(String url, long time) {
        logger.info(String.format("Completely crawled in time %d: %s", time, url));
    }

    public static void pushToQueue(String url, long time) {
        logger.info(String.format("pushed to queue in time: %d %s", time, url));
    }

    public static void writeRequestTimeLog(long requestTime, String link) {
        logger.info("Request time is " + requestTime + ", for link : " + link);
    }

    public static void writeResponseTimeLog(long responseTime, String link) {
        logger.info("Response time is " + responseTime + ", for link : " + link);
    }

    public static void writeCheckHeaderTimeLog(long checkHeaderTime, String link) {
        logger.info("Checked header in " + checkHeaderTime + ", for link : " + link);
    }

    public static void dataSentLog(String link, long sentTime) {
        logger.info("Data sent to hbase and kafka in time " + sentTime + ", link: " + link);
    }

    public static void downloadThread(String link, long downloadTime) {
        logger.info("Download thread done in time " + downloadTime + ", link: " + link);
    }

    public static void getLinkFinished(String linkToVisit, long timeDifference) {
        logger.info(String.format("fetched link is okay and send to download %d: %s", timeDifference, linkToVisit));
    }

    public static void htmlCheck(String link, long time) {
        logger.info("Html checked in time: " + time + ", link: " + link);
    }

    public static void pushBackToKafka(String link, long time) {
        logger.info("Error occurred after download and pushed back to kafka in time: " + time + ", link: " + link);
    }

    public static void logGapTime(String link, long gapTime) {
        logger.info("Gap time after download html and before put to downloaded queue: " + gapTime + ", link: " + link);
    }

    public static void popDownloadedData(String link, long time)
    {
        logger.info("Popped data from downloaded data queue in time: " + time + ", link: " + link);
    }

    public static void putOrganizedData(String link, long time)
    {
        logger.info("Sent PageInfo object to organized queue in time: " + time + ", link: " + link);
    }

    public static void organized(String link, long time)
    {
        logger.info("Organizing thread finished in time: " + time + ", link" + link);
    }

    public static void popOrganizedData(String link, long time)
    {
        logger.info("Popped data from organized queue in time: " + time + ", link: " + link);
    }
}
