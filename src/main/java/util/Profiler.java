package util;

import org.apache.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;


public class Profiler {
    private static AtomicLong consumedFromKafka = new AtomicLong(0);
    private static AtomicLong polites = new AtomicLong(0);
    private static AtomicLong impolite = new AtomicLong(0);
    private static AtomicLong goodLanguage = new AtomicLong(0);
    private static AtomicLong crawled = new AtomicLong(0);
    private static AtomicLong allCrawled = new AtomicLong(0);
    private static AtomicLong goodContentType = new AtomicLong(0);
    private static AtomicLong queueSize = new AtomicLong(0);
    private static AtomicLong notYetSize = new AtomicLong(0);
    private static Logger logger = Logger.getLogger(Class.class.getName());

    public static void start() {

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        System.out.println();
                        System.out.println("queue size: " + queueSize);
                        System.out.println("number of fetched links from queue to crawl: " + consumedFromKafka);
                        System.out.println("number of polite domains: " + polites);
                        System.out.println("number of impolite domains: " + impolite);
                        System.out.println("number of good content type: " + goodContentType);
                        System.out.println("number of links with english language: " + goodLanguage);
                        System.out.println("number of crawled links: " + crawled);
                        System.out.println("number of active threads: " + Thread.activeCount());
                        System.out.println("number of all crawled links: " + allCrawled);
                        System.out.println("not yet queue size: " + notYetSize);
                        System.out.println();

                        consumedFromKafka.set(0);
                        goodContentType.set(0);
                        polites.set(0);
                        impolite.set(0);
                        goodLanguage.set(0);
                        crawled.set(0);
                    }
                }, 0, 1000);
            }
        });
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
    }

    public static void getLinkFromQueueToCrawl(String url, long time) {
        if (time != 0)
        logger.info(String.format("Got link from queue in time %d: %s", time, url));
        consumedFromKafka.incrementAndGet();
    }

    public static void checkPolitensess(String url, long time, boolean isPolite) {
        String politeness = (isPolite ? "is polite": "not polite");
        if (time != 0)
        logger.info(String.format("Checked Politeness (%s) in time %d: %s", politeness, time, url));
        if (isPolite) polites.incrementAndGet();
    }

    public static void checkContentType(String url, long time, boolean isGood) {
        String goodness = (isGood ? "good": "bad");
        if (time != 0)
            logger.info(String.format("Checked content type (%s) in time %d: %s", isGood, time, url));
        if (isGood) goodContentType.incrementAndGet();
    }

    public static void isImpolite() {
        impolite.incrementAndGet();
    }

    public static void goodLanguage(String url, long time, boolean isEnglish) {
        String beingEnglish = (isEnglish ? "is english": "not english");
        if (time != 0)
        logger.info(String.format("Checked good language (%s) in time %d: %s", beingEnglish, time, url));
        if (isEnglish) goodLanguage.incrementAndGet();
    }

    public static void extractInformationFromDocument(String url, long time) {
        logger.info(String.format("Extracted info from document in time %d: %s", time, url));
        allCrawled.incrementAndGet();
        crawled.incrementAndGet();
    }

    public static void download(String url, long time) {
        if (time != 0)
            logger.info(String.format("Downloaded in time %d: %s", time, url));
    }

    public static void parse(String url, long time) {
        if (time != 0)
        logger.info(String.format("Parsed in time %d: %s", time, url));
    }

    public static void putToDataStore(String url, long time) {
        if (time != 0)
        logger.info(String.format("Putted in data store in time %d: %s", time, url));
    }

    public static void checkExistenceInDataStore(String url, long time, boolean isExists) {
        String existence = (isExists ? "exists": "does'nt exist");
        if (time != 0)
        logger.info(String.format("Check existence in data store (%s) in time %d: %s", existence, time, url));
    }

    public static void setQueueSize(long size) {
        queueSize.set(size);
    }

    public static void setNotYetSize(long size){
        notYetSize.set(size);
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
        logger.info("Checked header in "+ checkHeaderTime + ", for link : " + link);
    }

    public static void dataSentLog(String link, long sentTime)
    {
        logger.info("Data sent to hbase and kafka in time " + sentTime + ", link: " + link);
    }

    public static void downloadThread(String link, long downloadTime)
    {
        logger.info("Download thread done in time " + downloadTime + ", link: " + link);
    }
}
