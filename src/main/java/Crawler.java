import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import util.*;
import datastore.DataStore;
import datastore.LocalDataStore;
import datastore.PageInfo;
import datastore.PageInfoDataStore;
import javafx.util.Pair;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import queue.LocalQueue;
import queue.DistributedQueue;
import queue.URLQueue;

import java.io.*;
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
        configClient();

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

    private void configClient() {
        client.setReadTimeout(1500, TimeUnit.MILLISECONDS);
        client.setConnectTimeout(1500, TimeUnit.MILLISECONDS);
        client.setWriteTimeout(1500, TimeUnit.MILLISECONDS);
        client.setFollowRedirects(false);
        client.setFollowSslRedirects(false);
        client.setRetryOnConnectionFailure(false);
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
            long t1, time, startCrawlTime;

            Profiler.setQueueSize(queue.size());
            // pop from queue
            startCrawlTime = System.currentTimeMillis();
            try {
                linkToVisit = queue.pop();
                if (linkToVisit == null || linkToVisit.startsWith("ftp") || linkToVisit.startsWith("mailto")) continue;
                Profiler.getLinkFromQueueToCrawl();
            } catch (InterruptedException e) {
                System.err.println("error in reading from blocking queue: ");
                continue;
            }

            // check politeness
            boolean isPolite = isPolite(linkToVisit);
            Profiler.isPolite();
            if (!isPolite) {
                queue.push(linkToVisit);
                continue;
            }

            // check repeated
            try {
                boolean isExists = dataStore.exists(normalizeUrl(linkToVisit));
                if (isExists) continue;
                Profiler.isUnique();
            } catch (IOException e) {
                System.err.println("error in check existing in hbase: " + e);
            }

            if (!isGoodContentType(linkToVisit)) continue;

            // make connection and get response
            String html;
            try {
                html = getPureHtmlFromLink(linkToVisit);
                if (html == null) continue;
            } catch (IOException e) {
                continue;
            } catch (IllegalArgumentException e) {
                continue;
            }

            // parse html
            Document document = parseHtml(html);

            // check language
            boolean isEnglish = isEnglish(document);
            if (!isEnglish) continue;
            Profiler.isGoodLanguage();

            // extract info
            PageInfo pageInfo = getPageInfo(linkToVisit, document);

            try {
                dataStore.put(pageInfo);
            } catch (IOException e) {
                System.err.println("errrrror");
                System.exit(3);
            }

            ArrayList<String> sublinks = getAllSublinksFromPageInfo(pageInfo);
            queue.push(sublinks);

            Profiler.crawled(linkToVisit, System.currentTimeMillis() - startCrawlTime);
        }
    }

    private boolean isPolite(String stringUrl) {
        int index = stringUrl.indexOf("/", 8);
        if (index == -1) index = stringUrl.length();
        return !cache.checkIfExist(stringUrl.substring(0, index));
    }

    private boolean isGoodContentType(String url) {
        return !url.endsWith(".jpg") && !url.endsWith(".gif") && !url.endsWith(".pdf") && !url.endsWith(".deb")
                && !url.endsWith(".jpeg") && !url.endsWith(".png") && !url.endsWith(".txt") && !url.endsWith(".exe")
                && !url.endsWith(".gz") && !url.endsWith(".rar") && !url.endsWith(".zip") && !url.endsWith(".tar.gz");
    }

    private boolean isEnglish(Document document) {
        LanguageDetector languageDetector = new LanguageDetector(document);
        return languageDetector.isEnglish();
    }

    private String getPureHtmlFromLink(String link) throws IOException {
        long t1 = System.currentTimeMillis();
        Request request = new Request.Builder().url(link).build();
        Response response = client.newCall(request).execute();

        if (!response.header("Content-type", "text/html").startsWith("text/html")) {
            response.body().close();
            return null;
        }

        String html = response.body().string();
        response.body().close();
        if (html.length() == 0) return null;
        Profiler.download(link, html.length(), System.currentTimeMillis() - t1);
        return html;
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
                if (!href.equals("") && !href.startsWith("mailto") && !href.startsWith("ftp")) {
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
        data.setUrl(normalizeUrl(stringUrl));
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

    private String normalizeUrl(String url) {
        StringBuilder normalizedUrl = new StringBuilder("");
        url = url.toLowerCase();
        if (url.startsWith("http") || url.startsWith("https")) {
            int i = 0;
            while (url.charAt(i) != '/')
                i++;
            i += 2;
            for (; i < url.length(); i++) {
                if (i == url.length() - 1 && url.charAt(i) == '/')
                    break;
                normalizedUrl.append(url.charAt(i));
            }
            return normalizedUrl.toString();
        } else if (url.startsWith("ftp"))
            return null;
        else
            return url;
    }
}
