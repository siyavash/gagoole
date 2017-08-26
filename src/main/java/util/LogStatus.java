package util;

import org.apache.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;


public class LogStatus {
    private static int consumedFromKafka = 0;
    private static int polites = 0;
    private static int impolite = 0;
    private static int goodContentType = 0;
    private static int goodLanguage = 0;
    private static int processed = 0;
    private static int uniqueUrls = 0;

    private static int prevConsumedFromKafka = 0;
    private static int prevPolites = 0;
    private static int prevImpolite = 0;
    private static int prevGoodContentType = 0;
    private static int prevGoodLanguage = 0;
    private static int prevProcessed = 0;
    private static int prevUniqueurls = 0;

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

                        System.out.println("number of consumed links from kafka: " + (consumedFromKafka - prevConsumedFromKafka));
                        logger.info("number of consumed links from kafka: " + (consumedFromKafka - prevConsumedFromKafka));
                        prevConsumedFromKafka = consumedFromKafka;

                        System.out.println("number of polite domains: " + (polites - prevPolites));
                        logger.info("number of polite domains: " + (polites - prevPolites));
                        prevPolites = polites;

                        System.out.println("number of impolite domains: " + (impolite - prevImpolite));
                        logger.info("number of polite domains: " + (impolite - prevImpolite));
                        prevImpolite = impolite;

                        System.out.println("number of links having suitable content-type: " + (goodContentType - prevGoodContentType));
                        logger.info("number of links having suitable content-type: " + (goodContentType - prevGoodContentType));
                        prevGoodContentType = goodContentType;

                        System.out.println("number of links with english language: " + (goodLanguage - prevGoodLanguage));
                        logger.info("number of links with english language: " + (goodLanguage - prevGoodLanguage));
                        prevGoodLanguage = goodLanguage;

                        System.out.println("number of processed links: " + (processed - prevProcessed));
                        logger.info("number of processed links: " + (processed - prevProcessed));
                        prevProcessed = processed;

                        System.out.println("number of unique subUrls: " + (uniqueUrls - prevUniqueurls));
                        logger.info("number of unique subUrls: " + (uniqueUrls - prevUniqueurls));
                        prevUniqueurls = uniqueUrls;

                        System.out.println("number of active threads: " + Thread.activeCount());
                        logger.info("number of active threads: " + Thread.activeCount());

                        System.out.println();
                    }
                }, 0, 1000);
            }
        });
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
    }

    public synchronized static void consumeLinkFromKafka() {
        consumedFromKafka++;
    }

    public synchronized static void isPolite() {
        polites++;
    }

    public synchronized static void isImPolite() {
        impolite++;
    }

    public synchronized static void goodContentType() {
        goodContentType++;
    }

    public synchronized static void goodLanguage() {
        goodLanguage++;
    }

    public synchronized static void processed() {
        processed++;
    }

    public synchronized static void newUniqueUrl() {
        uniqueUrls++;
    }

    public static Logger getLogger() {
        return logger;
    }
}
