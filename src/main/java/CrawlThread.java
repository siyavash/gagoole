import Util.Logger;
import com.google.common.net.InternetDomainName;
import javafx.util.Pair;
import kafka.KafkaPublish;
import Util.LanguageException;
import kafka.URLQueue;
import org.apache.hadoop.hbase.client.Table;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class CrawlThread extends Thread { //

    private final KafkaSubscribe kafkaSubscribe;
    private final KafkaPublish publisher = KafkaPublish.getInstance();
    private final LruCache cache;
    private final PageInfoDataStore hbase;
    private Table table = null;
    URLQueue publisher = new URLQueue();

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
                System.err.println("error in reading from blocking queue");
            }
            if (linkToVisit == null) continue;

            Logger.consumeLinkFromKafka();


            try {
                if (!isPolite(linkToVisit)) {
                    publisher.produceUrl(linkToVisit);
                    continue;
                }
            } catch (Exception exception) {
                System.out.println("failed to extract domain: " + linkToVisit);
                continue;
            }

            Logger.isPolite();

            // TODO: put language checker and content-type checker here

            try {
                Connector connector = new Connector(linkToVisit);
                Document document = connector.getDocument();
<<<<<<< HEAD:src/main/java/CrawlThread.java
                PageProcessor pageProcessor = new PageProcessor(linkToVisit, document);  // TODO: make pageprocessor static
                URLData data = pageProcessor.getUrlData();
                System.out.println(data.getTitle());
                System.out.println("trying to put urldata to hbase");
=======
                PageProcessor pageProcessor = new PageProcessor(linkToVisit, document);
                PageInfo data = pageProcessor.getUrlData();
>>>>>>> 1a6c7613587ba33dbdbb12a8f5a334a2fb93e824:src/main/java/CrawlExecutor.java
                hbase.put(data, table);
                System.out.println("done");

                Logger.processed();

                ArrayList<Pair<String, String>> insideLinks = pageProcessor.getAllInsideLinks();
                for (Pair<String, String> pair : insideLinks) {
                    String url = pair.getKey();
                    if (!existsInHbase(url)) {
                        publisher.produceUrl(url);
                        Logger.newUniqueUrls();
                    }
                }
            } catch (LanguageException ex) { // TODO: make try cathc blocks small
//                System.err.println("Language detection: " + ex.getUrl());
            } catch (SocketTimeoutException ex) { // TODO:
//                System.err.println("timeout: " + linkToVisit);
                publisher.produceUrl(linkToVisit);
            }catch (IOException e) {
                System.err.println("io exception: " + e + "\n" + linkToVisit);
            } catch (Exception e) {
                System.err.println("exception: " + e + linkToVisit);
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

    public CrawlThread(KafkaSubscribe kafkaSubscribe, LruCache lruCache, GagooleHBase hbase) {
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
