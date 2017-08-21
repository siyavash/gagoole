import Util.*;
import com.google.common.net.InternetDomainName;
import datastore.DataStore;
import datastore.LocalDataStore;
import datastore.PageInfo;
import datastore.PageInfoDataStore;
import javafx.util.Pair;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import queue.BlockingQueue;
import queue.DistributedQueue;
import queue.URLQueue;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

class Crawler {

    private int NTHREADS;

    private final URLQueue queue;
    private final LruCache cache = new LruCache();
    private DataStore dataStore;
    private boolean initialMode = true;
    private boolean localMode = true;
    private String bootstrapServer;
    private String topicName;
    private String zookeeperClientPort;
    private String zookeeperQuorum;

    public Crawler() {
        loadProperties();
        LogStatus.getLogger().info("number of threads: " + NTHREADS);
        if (localMode) {
            queue = new BlockingQueue();
            dataStore = new LocalDataStore();
        } else {
            queue = new DistributedQueue(bootstrapServer, topicName);
            try {
                dataStore = new PageInfoDataStore(zookeeperClientPort, zookeeperQuorum);
            } catch (IOException e) {
                System.err.println("Error in initialising hbase: " + e);
                System.exit(1);
            }
        }

        if (initialMode) {
            ArrayList<String> seeds = loadSeeds();
            queue.push(seeds);

            if (!localMode) {
                System.out.println("seed has been published");
                System.exit(20);
            }
        } else if (!localMode) {
            queue.startThread();
        }
    }

    public void start() {
        ArrayList<Thread> threads = new ArrayList<Thread>(NTHREADS);
        for (int i = 0; i < NTHREADS; i++) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    runCrawlThread();
                }
            });
            thread.start();
            threads.add(thread);
        }
    }

    private void runCrawlThread() {
        while (true) {
            String linkToVisit;
            try {
                linkToVisit = queue.pop();
            } catch (InterruptedException e) {
                System.err.println("error in reading from blocking queue: ");
                continue;
            }
            LogStatus.consumeLinkFromKafka();

            try {
                if (!isPolite(linkToVisit)) {
                    LogStatus.isImPolite();
                    queue.push(linkToVisit);
                    continue;
                }
            } catch (IllegalArgumentException ex) {
//                System.out.println("failed to get domain: " + linkToVisit);
//                System.err.println("illegalArgument: " + linkToVisit);
                continue;
            } catch (IllegalStateException e){
//                System.err.println("illegalState: " + linkToVisit);
                continue;
            } catch (IOException e) {
//                System.err.println("io: " + linkToVisit);
                continue;
            }
            LogStatus.isPolite();

//            try {
//                if (!isGoodContentType(linkToVisit)) {
//                    // TODO: make log
//                    continue;
//                }
//            } catch (IOException e) {
//                // TODO: make log
//                continue;
//            }
//            LogStatus.goodContentType();

            Document document = null;
            try {
                document = getDocument(linkToVisit);
            } catch (IOException e) {
                // TODO: make log
                continue;
            } catch (IllegalArgumentException e) {
                continue;
            }

            if (!isEnglish(document)) continue;
            LogStatus.goodLanguage();

            PageInfo pageInfo = getPageInfo(linkToVisit, document);
            try {
                dataStore.put(pageInfo);
            } catch (IOException e) {
                System.err.println("errrrror");
                System.exit(3);
            }
            LogStatus.processed();

            ArrayList<String> sublinks = getAllSublinks(document);
            for (String link : sublinks) {
                try {
                    if (!dataStore.exists(link)) {
                        queue.push(link);
                        LogStatus.newUniqueUrl();
                    }
                } catch (IOException e) {

                }
            }
        }
    }

    private boolean isPolite(String stringUrl) throws IllegalArgumentException, IOException, IllegalStateException {
        URL url = new URL(stringUrl);
        String hostName = url.getHost();
        String domain = InternetDomainName.from(hostName).topPrivateDomain().toString();
        return !cache.checkIfExist(domain);
    }

    private boolean isGoodContentType(String urlString) throws IOException {
        String contentType;
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("HEAD");
        connection.connect();
        contentType = connection.getContentType();

        if (contentType == null)
            return true;

        return contentType.startsWith("text/html");
    }

    private boolean isEnglish(Document document) {
        LanguageDetector languageDetector = new LanguageDetector(document);
        return languageDetector.isEnglish();
    }

    private Document getDocument(String stringUrl) throws IOException, IllegalArgumentException {
        Connection.Response response = Jsoup.connect(stringUrl)
                .userAgent(UserAgents.getRandom())
                .maxBodySize(100 * 1024)
                .execute();
        return response.parse();
    }

    public ArrayList<Pair<String, String>> getAllSubLinksWithAnchor(Document document) {
        ArrayList<Pair<String, String>> insideLinks = new ArrayList<Pair<String, String>>();
        Elements elements = document.getElementsByTag("a");
        if (elements != null) {
            for (Element tag : elements) {
                String href = tag.absUrl("href");
                if (!href.equals("")) {
                    String anchor = tag.text();
                    insideLinks.add(new Pair<String, String>(href, anchor));
                }
            }
        }

        return insideLinks;
    }

    ArrayList<String> getAllSublinks(Document document) {
        ArrayList<String> insideLinks = new ArrayList<String>();
        Elements elements = document.getElementsByTag("a");
        if (elements != null) {
            for (Element tag : elements) {
                String href = tag.absUrl("href");
                if (!href.equals(""))
                    insideLinks.add(href);
            }
        }

        return insideLinks;
    }

    PageInfo getPageInfo(String stringUrl, Document document) {
        PageInfo data = new PageInfo();
        data.setUrl(stringUrl);
        data.setSubLinks(getAllSubLinksWithAnchor(document));
        data.setTitle(document.title());
        if (document.body() != null)
            data.setBodyText(document.body().text());
        data.setMeta(document.getElementsByTag("meta"));
        return data;
    }

    private void loadProperties() {
        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = new FileInputStream("config.properties");
            prop.load(input);

            NTHREADS = Integer.parseInt(prop.getProperty("threads-number", "500"));
            initialMode = prop.getProperty("initial-mode", "true").equals("true");
            localMode = prop.getProperty("local-mode", "true").equals("true");
            bootstrapServer = prop.getProperty("bootstrap-server", "master:9092, slave:9092");
            topicName = prop.getProperty("topic-name", "test");
            zookeeperClientPort = prop.getProperty("zookeeper-client-port", "2181");
            zookeeperQuorum = prop.getProperty("zookeeper-quorum", "master,slave");
        } catch (IOException ex) {
            System.err.println("error in reading config file:");
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private ArrayList<String> loadSeeds() {
        try {
            ArrayList<String> seedUrls = new ArrayList<String>(500);
            File file = new File("seed.txt");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                seedUrls.add("http://www." + line);
            }
            fileReader.close();
            return seedUrls;
        } catch (IOException e) {
            System.err.println("error in loading seed: " + e);
            System.exit(1);
            return null;
        }
    }

}
