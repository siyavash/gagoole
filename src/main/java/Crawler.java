import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import util.*;
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
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
    private static OkHttpClient client = new OkHttpClient();

    public Crawler() {
        loadProperties();
        loadQueue();
        loadDataStore();
        client.setConnectTimeout(10, TimeUnit.SECONDS);

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

            // pop from queue
            try {
                t1 = System.currentTimeMillis();
                linkToVisit = queue.pop();
                if (linkToVisit == null) continue;
                time = System.currentTimeMillis() - t1;
                Profiler.getLinkFromQueueToCrawl(linkToVisit, time);
            } catch (InterruptedException e) {
                System.err.println("error in reading from blocking queue: ");
                continue;
            }

            // check politeness
            t1 = System.currentTimeMillis();
            boolean isPolite = isPolite(linkToVisit);
            time = System.currentTimeMillis() - t1;
            Profiler.checkPolitensess(linkToVisit, time, isPolite);
            if (!isPolite) {
                Profiler.isImpolite();
                queue.push(linkToVisit);
                continue;
            }

            // check repeated
            try {
                t1 = System.currentTimeMillis();
                boolean isExists = dataStore.exists(linkToVisit);
                time = System.currentTimeMillis() - t1;
                Profiler.checkExistenceInDataStore(linkToVisit, time, isExists);
                if (isExists) continue;
            } catch (IOException e) {
                System.err.println("error in check existing in hbase: " + e);
            }

            // check content-type
            try {
                t1 = System.currentTimeMillis();
                boolean isGoodContentType = isGoodContentType(linkToVisit);
                time = System.currentTimeMillis() - t1;
                Profiler.checkContentType(linkToVisit, time, isGoodContentType);
            } catch (IOException e) {
//                e.printStackTrace();
                continue;
            }

            // make connection and get response
            String html;
            try {
                t1 = System.currentTimeMillis();
                html = getPureHtmlFromLink(linkToVisit);
                time = System.currentTimeMillis() - t1;
                Profiler.download(linkToVisit, time);
            } catch (IOException e) {
                continue;
            } catch (IllegalArgumentException e) {
                continue;
            }

            // parse html
            Document document;
            t1 = System.currentTimeMillis();
            document = parseHtml(html);
            time = System.currentTimeMillis() - t1;
            Profiler.parse(linkToVisit, time);

            // check language
            t1 = System.currentTimeMillis();
            boolean isEnglish = isEnglish(document);
            time = System.currentTimeMillis() - t1;
            Profiler.goodLanguage(linkToVisit, time, isEnglish);
            if (!isEnglish) continue;

            // extract info
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

            ArrayList<String> sublinks = getAllSublinksFromPageInfo(pageInfo);
            queue.push(sublinks);
        }
    }

    private boolean isPolite(String stringUrl) {
        int index = stringUrl.indexOf("/", 8);
        if (index == -1) index = stringUrl.length();
        return !cache.checkIfExist(stringUrl.substring(0, index));
    }

    private boolean isGoodContentType(String link) throws IOException {
        OkHttpClient client = new OkHttpClient();
        client.setConnectTimeout(10, TimeUnit.SECONDS);
        Request request = new Request.Builder().url(link).method("HEAD", null).build();
        Response response = client.newCall(request).execute();
        String contentType = response.header("Content-type", "text/html");
        return contentType.startsWith("text/html");
    }

    private boolean isEnglish(Document document) {
        LanguageDetector languageDetector = new LanguageDetector(document);
        return languageDetector.isEnglish();
    }

    private static String getPureHtmlFromLink(String link) throws IOException {
        Request request = new Request.Builder().url(link).build();
        Response response = client.newCall(request).execute();
        return response.body().string();
    }

    private Document parseHtml(String content) {
        return Jsoup.parse(content);
    }

    public ArrayList<Pair<String, String>> getAllSubLinksWithAnchor(Document document) {
        ArrayList<Pair<String, String>> insideLinks = new ArrayList<Pair<String, String>>();
        Elements elements = document.getElementsByTag("a");
        if (elements != null) {
            for (Element tag : elements) {
                String href = tag.absUrl("href");
                if (!href.equals("") && !href.startsWith("mailto")) {
                    String anchor = tag.text();
                    insideLinks.add(new Pair<String, String>(href, anchor));
                }
            }
        }

        return insideLinks;
    }

    ArrayList<String> getAllSublinksFromPageInfo(PageInfo pageInfo) {
        ArrayList<String> insideLinks = new ArrayList<String>();
        for (Pair<String, String> pair : pageInfo.getSubLinks()) {
            insideLinks.add(pair.getKey());
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

    public static void main(String[] args) throws Exception {
    }

}
