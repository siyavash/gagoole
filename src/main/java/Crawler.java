import util.*;
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
import queue.LocalQueue;
import queue.DistributedQueue;
import queue.URLQueue;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Properties;

class Crawler {

    private int NTHREADS;

    private URLQueue queue;
    private final LruCache cache = new LruCache();
    private DataStore dataStore;
    private boolean initialMode = true;
    private boolean useKafka = false;
    private boolean useHbase = false;
    private String bootstrapServer;
    private String topicName;
    private String zookeeperClientPort;
    private String zookeeperQuorum;

    public Crawler() {
        loadProperties();
        loadQueue();
        loadDataStore();

        if (initialMode && useKafka) {
            ArrayList<String> seeds = loadSeeds();
            queue.push(seeds);
            queue.close();
            System.out.println("seed has been published");
            System.exit(20);
        } else if (initialMode && !useKafka) {
            ArrayList<String> seeds = loadSeeds();
            queue.push(seeds);
        } else if (!initialMode && useKafka) {
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

    private void loadQueue() {
        if (useKafka) {
            queue = new DistributedQueue(bootstrapServer, topicName);
        } else {
            queue = new LocalQueue();
        }
    }

    private void loadDataStore() {
        if (useHbase) {
            try {
                dataStore = new PageInfoDataStore(zookeeperClientPort, zookeeperQuorum);
            } catch (IOException e) {
                System.err.println("Error in initialising hbase: " + e);
                System.exit(1);
            }
        } else {
            dataStore = new LocalDataStore();
        }
    }

    private void runCrawlThread() {
        while (true) {
            String linkToVisit;
            long t1, time;

            try {
                t1 = System.currentTimeMillis();
                linkToVisit = queue.pop();
                time = System.currentTimeMillis() - t1;
                Profiler.getLinkFromQueueToCrawl(linkToVisit, time);
            } catch (InterruptedException e) {
                System.err.println("error in reading from blocking queue: ");
                continue;
            }

            try {
                t1 = System.currentTimeMillis();
                boolean isPolite = isPolite(linkToVisit);
                time = System.currentTimeMillis() - t1;
                Profiler.checkPolitensess(linkToVisit, time, isPolite);
                if (!isPolite) {
                    Profiler.isImpolite();
                    queue.push(linkToVisit);
                    continue;
                }
            } catch (IllegalArgumentException ex) {
                continue;
            } catch (IllegalStateException e){
                continue;
            } catch (IOException e) {
                continue;
            }

            try {
                t1 = System.currentTimeMillis();
                boolean isExists = dataStore.exists(linkToVisit);
                time = System.currentTimeMillis() - t1;
                Profiler.checkExistenceInDataStore(linkToVisit, time, isExists);
                if (isExists) continue;
            } catch (IOException e) {
                System.err.println("error in check existing in hbase: " + e);
            }

            try {
                t1 = System.currentTimeMillis();
                boolean isGoodContentType = isGoodContentType(linkToVisit);
                time = System.currentTimeMillis() - t1;
                Profiler.checkContentType(linkToVisit, time, isGoodContentType);
                if (!isGoodContentType) continue;
            } catch (IOException e) {
                continue;
            } catch (IllegalArgumentException e) {
                continue;
            }

            Document document;
            try {
                document = getDocument(linkToVisit);
            } catch (IOException e) {
                continue;
            } catch (IllegalArgumentException e) {
                continue;
            }

            t1 = System.currentTimeMillis();
            boolean isEnglish = isEnglish(document);
            time = System.currentTimeMillis() - t1;
            Profiler.goodLanguage(linkToVisit, time, isEnglish);
            if (!isEnglish) continue;

            t1 = System.currentTimeMillis();
            PageInfo pageInfo = getPageInfo(linkToVisit, document);
            time = System.currentTimeMillis() - t1;
            Profiler.extractInformationFromDocument(linkToVisit, time);

            try {
                t1 = System.currentTimeMillis();
                dataStore.put(pageInfo);
                time = System.currentTimeMillis() - t1;
                Profiler.putToDataStore(linkToVisit, time);
            } catch (IOException e) {
                System.err.println("errrrror");
                System.exit(3);
            }

            ArrayList<String> sublinks = getAllSublinks(document);
            for (String link : sublinks) {
                try {
                    t1 = System.currentTimeMillis();
                    boolean isExists = dataStore.exists(link);
                    time = System.currentTimeMillis() - t1;
                    Profiler.checkExistenceInDataStore(link, time, isExists);
                    if (!isExists) {
                        queue.push(link);
                        Profiler.newUniqueUrl();
                    }
                } catch (IOException e) {
                    System.err.println("error in check existing in hbase: " + e);
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
        URLConnection connection = url.openConnection();
        if (!(connection instanceof HttpURLConnection)) return false;
        HttpURLConnection httpURLConnection = (HttpURLConnection) connection;
        httpURLConnection.setRequestMethod("HEAD");
        httpURLConnection.connect();
        contentType = httpURLConnection.getContentType();

        if (contentType == null)
            return true;

        return contentType.startsWith("text/html");
    }

    private boolean isEnglish(Document document) {
        LanguageDetector languageDetector = new LanguageDetector(document);
        return languageDetector.isEnglish();
    }

    private Document getDocument(String stringUrl) throws IOException, IllegalArgumentException {
        long t1 = System.currentTimeMillis();
        Connection.Response response = Jsoup.connect(stringUrl)
                .userAgent(UserAgents.getRandom())
                .execute();
        long time = System.currentTimeMillis() - t1;
        Profiler.download(stringUrl, time, response.bodyAsBytes().length);

        t1 = System.currentTimeMillis();
        Document document = response.parse();
        time = System.currentTimeMillis() - t1;
        Profiler.parse(stringUrl, time);

        return document;
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

        Elements elements = document.select("meta[name=author]");
        if (elements != null)
            data.setAuthorMeta(elements.attr("content"));

        elements = document.select("meta[name=description]");
        if (elements != null)
            data.setDescriptionMeta(elements.attr("content"));

        elements = document.select("meta[name=content-type]");
        if (elements != null)
            data.setContentTypeMeta(elements.attr("content"));

        elements = document.select("meta[name=keywords]");
        if (elements != null)
            data.setKeyWordsMeta(elements.attr("content"));

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
            useKafka = prop.getProperty("use-kafka", "false").equals("true");
            useHbase = prop.getProperty("use-hbase", "false").equals("true");
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
