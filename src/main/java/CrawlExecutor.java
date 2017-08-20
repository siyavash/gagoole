import javafx.util.Pair;
import kafka.KafkaPublish;
import kafka.KafkaSubscribe;
import Util.LanguageException;
import org.apache.hadoop.hbase.client.Table;
import org.jsoup.HttpStatusException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class CrawlExecutor implements Runnable {

    private final KafkaSubscribe kafkaSubscribe;
    private final LruCache cache;
    private final GagooleHBase hbase;
    private Table table = null;

    public void run() {

        KafkaPublish publisher = KafkaPublish.getInstance();
        ArrayBlockingQueue<String> arrayBlockingQueue = kafkaSubscribe.getUrlsArrayBlockingQueue();
        try {
            table = hbase.getTable();
        } catch (IOException e) {
            System.err.println("error in getting table");
            e.printStackTrace();
            System.exit(10);
        }

        while (true) {
            String linkToVisit = null;
            try {
                linkToVisit = arrayBlockingQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (linkToVisit == null) continue;

            DomainHandler domainHandler = new DomainHandler(linkToVisit);
            String domain = domainHandler.getDomain();
            if (domain == null) continue;

            if (!cache.checkIfExist(domain)) {
                publisher.produceUrl(linkToVisit);
                continue;
            }

            try {
                PageProcessor pageProcessor = new PageProcessor(linkToVisit);
                URLData data = pageProcessor.getUrlData();
                hbase.put(data, table);
                ArrayList<Pair<String, String>> insideLinks = pageProcessor.getAllInsideLinks();
                for (Pair<String, String> pair : insideLinks) {
                    String url = pair.getKey();
                    if (!hbase.exists(url, table))
                        publisher.produceUrl(url);
                }
            } catch (LanguageException ex) {
                System.err.println("Language detection: " + ex.getUrl());
            } catch (SocketTimeoutException ex) {
                System.err.println("timeout: " + linkToVisit);
                publisher.produceUrl(linkToVisit);
            } catch (HttpStatusException statusException) {
                int statusCode = statusException.getStatusCode();
                switch (statusCode) {
                    case 500:
                        System.err.println("handled error 500: " + linkToVisit);
                        publisher.produceUrl(linkToVisit);
                        break;
                    case 503:
                        System.err.println("handled error 503: " + linkToVisit);
                        publisher.produceUrl(linkToVisit);
                        break;
                    case 502:
                        System.err.println("handled error 502: " + linkToVisit);
                        publisher.produceUrl(linkToVisit);
                        break;
                    case 404:
                        break;
                    default:
                        System.err.println("Error in connecting url: " + linkToVisit + " " + statusException);
                        break;
                }
            } catch (IOException e) {
                System.err.println("io exception" + e + "\n" + linkToVisit);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public CrawlExecutor(KafkaSubscribe kafkaSubscribe, LruCache lruCache, GagooleHBase hbase) {
        this.hbase = hbase;
        this.cache = lruCache;
        this.kafkaSubscribe = kafkaSubscribe;
    }

    private boolean existsInHbase(String url) {
        try {
            return hbase.exists(url, table);
        } catch (IOException e) {
            System.err.println("Error in check exising in hbase");
            e.printStackTrace();
            System.exit(11);
        }

        return true;
    }
}
