package util;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class Profiler
{
    private static Logger logger = Logger.getLogger(Class.class.getName());
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Meter existChecks = metrics.meter("Exist checked");
    private static final Meter existCheckFails = metrics.meter("Exist check failed");
    private static final Meter downloaded = metrics.meter("Download done");
    private static final Meter failedDownload = metrics.meter("Download failed");
    private static final Meter puts = metrics.meter("Put done");
    private static final Meter organized = metrics.meter("Organized");

    private static AtomicLong linkedSize = new AtomicLong(0);

    public static void start() {
        metrics.register(MetricRegistry.name("linked size"),
                (Gauge<Long>) () -> linkedSize.get());

        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .build();

        reporter.start(1, TimeUnit.SECONDS);
    }


    public static void setLinkedSize(int size) {
        linkedSize.set(size);
    }

    /*****************************************************************************/

    public static void existChecked()
    {
        existChecks.mark();
    }

    public static void existCheckFail()
    {
        existCheckFails.mark();
    }

    public static void downloadDone()
    {
        downloaded.mark();
    }

    public static void putDone(long numberOfPuts)
    {
        puts.mark(numberOfPuts);
    }

    public static void organizeDone()
    {
        organized.mark();
    }

    public static void downloadFailed()
    {
        failedDownload.mark();
    }

    public static void error(String message)
    {
        logger.error(message);
    }

    public static void fatal(String message)
    {
        logger.fatal(message);
    }
}