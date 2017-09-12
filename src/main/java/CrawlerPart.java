import java.util.concurrent.ExecutorService;

public abstract class CrawlerPart
{
    private ExecutorService executorService;

    public abstract void startThreads();

    public void close()
    {
        if (executorService != null)
        {
            executorService.shutdownNow();
        }
    }

    public void setExecutorService(ExecutorService executorService)
    {
        this.executorService = executorService;
    }
}
