import datastore.DataStore;
import util.Profiler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NewUrlFilter extends CrawlerPart
{

    private DataStore urlDatabase;
    private ArrayBlockingQueue<String> properUrls;
    private ArrayBlockingQueue<String> newUrls;
    private final int THREAD_NUMBER;

    public NewUrlFilter(DataStore urlDatabase, ArrayBlockingQueue<String> properUrls, ArrayBlockingQueue<String> newUrls)
    {
        this.urlDatabase = urlDatabase;
        this.properUrls = properUrls;
        this.newUrls = newUrls;
        THREAD_NUMBER = readProperty();
    }

    private int readProperty()
    {
        Properties prop = new Properties();

        try (InputStream input = new FileInputStream("config.properties"))
        {
            prop.load(input);
        } catch (IOException ex)
        {
            Profiler.error("Error while reading config file");
        }

        return Integer.parseInt(prop.getProperty("check-exist-threads-number", "200"));
    }

    @Override
    public void startThreads()
    {
        if (THREAD_NUMBER == 0)
        {
            return;
        }

        ExecutorService checkingPool = Executors.newFixedThreadPool(THREAD_NUMBER);

        for (int i = 0; i < THREAD_NUMBER; i++)
        {
            checkingPool.submit((Runnable) () -> {
                while (true)
                {

                    try
                    {
                        ArrayList<String> urlsToVisit = new ArrayList<>();
                        for (int j = 0; j < 10; j++)
                        {
                            String urlToVisit = properUrls.take();
                            if (urlToVisit != null)
                            {
                                urlsToVisit.add(urlToVisit);
                            }
                        }


                        boolean[] existInDataStore = urlDatabase.exists(urlsToVisit);

                        for (int j = 0; j < 10; j++)
                        {
                            if (!existInDataStore[j])
                            {
                                Profiler.falseExistence();
                                newUrls.put(urlsToVisit.get(j));
                            } else
                            {
                                Profiler.exist();
                            }
                        }

                    } catch (InterruptedException ignored)
                    {
                        break;
                    } catch (IOException e)
                    {
                        Profiler.error("Error in checking the existence of a url");
                    }


                }
            });
        }
        checkingPool.shutdown();

        setExecutorService(checkingPool);
    }
}