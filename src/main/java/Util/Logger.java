package Util;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by Amir on 8/20/2017 AD.
 */
public class Logger {
    private static int consumedFromKafka = 0;
    private static int polites = 0;
    private static int goodContentType = 0;
    private static int goodLanguage = 0;
    private static int processed = 0;
    private static int uniqueUrls = 0;

    private static int prevConsumedFromKafka = 0;
    private static int prevPolites = 0;
    private static int prevGoodContentType = 0;
    private static int prevGoodLanguage = 0;
    private static int prevProcessed = 0;
    private static int prevUniqueurls = 0;

    public static void start() {

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println();

                System.out.println("number of consumed links from kafka: " + (consumedFromKafka - prevConsumedFromKafka));
                prevConsumedFromKafka = consumedFromKafka;

                System.out.println("number of polite domains: " + (polites - prevPolites));
                prevPolites = polites;

                System.out.println("number of links having suitable content-type: " + (goodContentType - prevGoodContentType));
                prevGoodContentType = goodContentType;

                System.out.println("number of links with english language: " + (goodLanguage - prevGoodLanguage));
                prevGoodLanguage = goodLanguage;

                System.out.println("number of processed links: " + (processed - prevProcessed));
                prevProcessed = processed;

                System.out.println("number of unique subUrls: " + (uniqueUrls - prevUniqueurls));
                prevUniqueurls = uniqueUrls;

                System.out.println("number of active threads: " + Thread.activeCount());

                System.out.println();
            }
        }, 0, 1000);
    }

    public synchronized static void consumeLinkFromKafka() {
        consumedFromKafka++;
    }

    public synchronized static void isPolite() {
        polites++;
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

    public synchronized static void newUniqueUrls() {
        uniqueUrls++;
    }
}
