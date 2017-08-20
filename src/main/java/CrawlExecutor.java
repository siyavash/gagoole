import Util.Logger;
import com.google.common.net.InternetDomainName;
import javafx.util.Pair;
import kafka.KafkaPublish;
import kafka.KafkaSubscribe;
import Util.LanguageException;
import org.apache.hadoop.hbase.client.Table;
import org.jsoup.HttpStatusException;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class CrawlExecutor extends Thread {

    private final KafkaSubscribe kafkaSubscribe;
    private final KafkaPublish publisher = KafkaPublish.getInstance();
    private final LruCache cache;
    private final GagooleHBase hbase;
    private Table table = null;

    public void run() {

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

            Logger.consumeLinkFromKafka();


            try {
                if (!isPolite(linkToVisit)) {
                    publisher.produceUrl(linkToVisit);
                    continue;
                }
            } catch (Exception exception) {
                continue;
            }

            Logger.isPolite();

            try {
                Connector connector = new Connector(linkToVisit);
                Document document = connector.getDocument();
                PageProcessor pageProcessor = new PageProcessor(linkToVisit, document);
                URLData data = pageProcessor.getUrlData();
                hbase.put(data, table);

                Logger.processed();

                ArrayList<Pair<String, String>> insideLinks = pageProcessor.getAllInsideLinks();
                for (Pair<String, String> pair : insideLinks) {
                    String url = pair.getKey();
                    if (!existsInHbase(url)) {
                        publisher.produceUrl(url);
                        Logger.newUniqueUrls();
                    }
                }
            } catch (LanguageException ex) {
//                System.err.println("Language detection: " + ex.getUrl());
            } catch (SocketTimeoutException ex) {
//                System.err.println("timeout: " + linkToVisit);
                publisher.produceUrl(linkToVisit);
            } catch (HttpStatusException statusException) {
                int statusCode = statusException.getStatusCode();
                switch (statusCode) {
                    case 500:
//                        System.err.println("handled error 500: " + linkToVisit);
                        publisher.produceUrl(linkToVisit);
                        break;
                    case 503:
//                        System.err.println("handled error 503: " + linkToVisit);
                        publisher.produceUrl(linkToVisit);
                        break;
                    case 502:
//                        System.err.println("handled error 502: " + linkToVisit);
                        publisher.produceUrl(linkToVisit);
                        break;
                    case 404:
                        break;
                    default:
//                        System.err.println("Error in connecting url: " + linkToVisit + " " + statusException);
                        break;
                }
            } catch (IOException e) {
//                System.err.println("io exception" + e + "\n" + linkToVisit);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isPolite(String stringUrl) throws Exception {
        URL url = new URL(stringUrl);
        String hostName = url.getHost();
        String domain = InternetDomainName.from(hostName).topPrivateDomain().toString();

        if (domain == null)
            throw new Exception();

        return !cache.checkIfExist(domain);

//        catch (MalformedURLException e) {
//            System.err.println("malformed url exception: " + stringUrl);
//            return null;
//        } catch (IllegalArgumentException ex) {
//            System.err.println("Illegal argument exception: " + stringUrl);
//            return null;
//        } catch (IllegalStateException ex) {
//            System.err.println("Illegal state exception: " + stringUrl);
//            return null;
//        }
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
