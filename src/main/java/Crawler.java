import com.squareup.okhttp.OkHttpClient;
import datastore.DataStore;
import datastore.LocalDataStore;
import datastore.PageInfoDataStore;
import javafx.util.Pair;
import queue.DistributedQueue;
import queue.LocalQueue;
import queue.URLQueue;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

class Crawler
{

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
    private OkHttpClient client = new OkHttpClient();
    private ArrayBlockingQueue<String> notYetDownloadedLinks = new ArrayBlockingQueue<>(100000);
    private ArrayBlockingQueue<Pair<String, String>> downloadedData = new ArrayBlockingQueue<>(100000);

    public Crawler()
    {
        loadProperties();
        loadQueue();
        loadDataStore();
        client.setReadTimeout(10, TimeUnit.SECONDS);

        if (initialMode && useKafka)
        {
            ArrayList<String> seeds = loadSeeds();
            queue.push(seeds);
            queue.close();
            System.out.println("seed has been published");
            System.exit(20);
        } else if (initialMode && !useKafka)
        {
            ArrayList<String> seeds = loadSeeds();
            queue.push(seeds);
        } else if (!initialMode && useKafka)
        {
            queue.startThread();
        }
    }

    public void start()
    {
        for (int i = 0; i < NTHREADS; i++)
        {
            new LinkFilterThread(queue, dataStore, client).start();
            new DataSenderThread(dataStore, queue, downloadedData).start();
        }
        for (int i = 0; i < 1000; i++) {
            new DownloadThread(notYetDownloadedLinks, downloadedData, client).start();
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
                System.err.println("Error in initialising hbase: " + e);
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
        InputStream input = null;

        try
        {

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
        } catch (IOException ex)
        {
            System.err.println("error in reading config file:");
            ex.printStackTrace();
        } finally
        {
            if (input != null)
            {
                try
                {
                    input.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
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
                seedUrls.add("http://www." + line);
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

    public String normalizeUrl(String url)
    {
        String normalizedUrl;
        url = url.toLowerCase();
        if (url.startsWith("ftp"))
            return null;
        normalizedUrl = url.replaceFirst("(www\\.)", "");
        int slashCounter = 0;
        if (normalizedUrl.endsWith("/"))
        {
            while (normalizedUrl.length() - slashCounter > 0 && normalizedUrl.charAt(normalizedUrl.length() - slashCounter - 1) == '/')
                slashCounter++;
        }
        normalizedUrl = normalizedUrl.substring(0, normalizedUrl.length() - slashCounter);
        return normalizedUrl;
    }
}
