package util;

import org.apache.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;


public class Profiler {
    private static long consumedFromKafka = 0;
    private static long polites = 0;
    private static long impolite = 0;
    private static long goodLanguage = 0;
    private static long crawled = 0;
    private static long allCrawled = 0;
    private static long goodContentType = 0;

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
                        System.out.println("number of fetched links from queue to crawl: " + consumedFromKafka);
                        System.out.println("number of polite domains: " + polites);
                        System.out.println("number of impolite domains: " + impolite);
                        System.out.println("number of good content type: " + goodContentType);
                        System.out.println("number of links with english language: " + goodLanguage);
                        System.out.println("number of crawled links: " + crawled);
                        System.out.println("number of active threads: " + Thread.activeCount());
                        System.out.println("number of all crawled links: " + allCrawled);
                        System.out.println();

                        consumedFromKafka = 0;
                        goodContentType = 0;
                        polites = 0;
                        impolite = 0;
                        goodLanguage = 0;
                        crawled = 0;
                    }
                }, 0, 1000);
            }
        });
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
    }

    public synchronized static void getLinkFromQueueToCrawl(String url, long time) {
        if (time != 0)
        logger.info(String.format("Got link from queue in time %d: %s", time, url));
        consumedFromKafka++;
    }

    public synchronized static void checkPolitensess(String url, long time, boolean isPolite) {
        String politeness = (isPolite ? "is polite": "not polite");
        if (time != 0)
        logger.info(String.format("Checked Politeness (%s) in time %d: %s", politeness, time, url));
        if (isPolite) polites++;
    }

    public synchronized static void checkContentType(String url, long time, boolean isGood) {
        String goodness = (isGood ? "good": "bad");
        if (time != 0)
            logger.info(String.format("Checked content type (%s) in time %d: %s", isGood, time, url));
        if (isGood) goodContentType++;
    }

    public synchronized static void isImpolite() {
        impolite++;
    }

    public synchronized static void goodLanguage(String url, long time, boolean isEnglish) {
        String beingEnglish = (isEnglish ? "is english": "not english");
        if (time != 0)
        logger.info(String.format("Checked good language (%s) in time %d: %s", beingEnglish, time, url));
        if (isEnglish) goodLanguage++;
    }

    public synchronized static void extractInformationFromDocument(String url, long time) {
        logger.info(String.format("Extracted info from document in time %d: %s", time, url));
        allCrawled++;
        crawled++;
    }

    public synchronized static void download(String url, long time) {
        if (time != 0)
            logger.info(String.format("Downloaded in time %d: %s", time, url));
    }

    public synchronized static void parse(String url, long time) {
        if (time != 0)
        logger.info(String.format("Parsed in time %d: %s", time, url));
    }

    public synchronized static void putToDataStore(String url, long time) {
        if (time != 0)
        logger.info(String.format("Putted in data store in time %d: %s", time, url));
    }

    public synchronized static void checkExistenceInDataStore(String url, long time, boolean isExists) {
        String existence = (isExists ? "exists": "does'nt exist");
        if (time != 0)
        logger.info(String.format("Check existence in data store (%s) in time %d: %s", existence, time, url));
    }

}
