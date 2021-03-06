import datastore.DataStore;
import datastore.LocalDataStore;
import datastore.PageInfo;
import datastore.PageInfoDataStore;
import javafx.util.Pair;
import queue.DistributedQueue;
import queue.LocalQueue;
import queue.URLQueue;
import util.Profiler;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.*;

class Crawler
{
    private URLQueue queue;
    private DataStore dataStore;
    private boolean initialMode = true;
    private boolean useKafka = false;
    private boolean useHbase = false;
    private String bootstrapServer;
    private String topicName;
    private String zookeeperClientPort;
    private String zookeeperQuorum;
    private ArrayBlockingQueue<String> properUrls = new ArrayBlockingQueue<>(5000);
    private ArrayBlockingQueue<String> newUrls = new ArrayBlockingQueue<>(500);
    private ArrayBlockingQueue<Pair<String, String>> downloadedData = new ArrayBlockingQueue<>(400);
    private ArrayBlockingQueue<PageInfo> organizedData = new ArrayBlockingQueue<>(400);
    private ArrayList<CrawlerPart> crawlerParts = new ArrayList<>();

    public Crawler()
    {
        loadProperties();
        loadQueue();
        loadDataStore();
        addCrawlerParts();
        startURLQueueThread();
    }

    private void startURLQueueThread()
    {
        if (initialMode && useKafka)
        {
            ArrayList<String> seeds = loadSeeds();
            queue.push(seeds);
            queue.close();
            System.out.println("seed has been published");
            System.exit(20);
        } else if (initialMode)
        {
            ArrayList<String> seeds = loadSeeds();
            queue.push(seeds);
        } else if (useKafka)
        {
            queue.startThread();
        }
    }

    private void addCrawlerParts()
    {
        crawlerParts.add(new ProperUrlFilter(queue, properUrls));
        crawlerParts.add(new NewUrlFilter(dataStore, properUrls, newUrls));
        crawlerParts.add(new HtmlCollector(newUrls, downloadedData));
        crawlerParts.add(new DataOrganizer(downloadedData, organizedData));
        crawlerParts.add(new DataSender(dataStore, queue, organizedData));
    }

    public void start()
    {
        for (CrawlerPart crawlerPart : crawlerParts)
        {
            crawlerPart.startThreads();
        }
    }

    private void loadQueue()
    {
        if (useKafka)
        {
            queue = new DistributedQueue(bootstrapServer, topicName);
        } else
        {
            queue = new LocalQueue();
        }
    }

    private void loadDataStore()
    {
        if (useHbase)
        {
            try
            {
                dataStore = new PageInfoDataStore(zookeeperClientPort, zookeeperQuorum);
            } catch (IOException e)
            {
                Profiler.fatal("Error in initializing hbase: " + e.getMessage());
                System.exit(1);
            }
        } else
        {
            dataStore = new LocalDataStore();
        }
    }

    private void loadProperties()
    {
        Properties prop = new Properties();

        try(InputStream input = new FileInputStream("config.properties"))
        {
            prop.load(input);

        } catch (IOException ex)
        {
            Profiler.error("Error while reading config file");
        }

        initialMode = prop.getProperty("initial-mode", "true").equals("true");
        useKafka = prop.getProperty("use-kafka", "false").equals("true");
        useHbase = prop.getProperty("use-hbase", "false").equals("true");
        bootstrapServer = prop.getProperty("bootstrap-server", "master:9092, slave:9092");
        topicName = prop.getProperty("topic-name", "shahrivar17th");
        zookeeperClientPort = prop.getProperty("zookeeper-client-port", "2181");
        zookeeperQuorum = prop.getProperty("zookeeper-quorum", "master,slave");
    }

    private ArrayList<String> loadSeeds()
    {
        try
        {
            ArrayList<String> seedUrls = new ArrayList<>(500);
            File file = new File("seed.txt");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null)
            {
                seedUrls.add("http://" + line);
            }
            fileReader.close();
            return seedUrls;
        } catch (IOException e)
        {
            System.err.println("error in loading seed: " + e);
            System.exit(1);
            return null;
        }
    }

    public void close()
    {
        for (CrawlerPart crawlerPart : crawlerParts)
        {
            crawlerPart.close();
        }
    }
}
