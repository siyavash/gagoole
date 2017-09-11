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
//    private static final Meter notExist = metrics.meter("Did not exist");
//    private static final Meter existNum = metrics.meter("Existed");
    private static final Meter downloaded = metrics.meter("Download done");
//    private static final Meter failedDownload = metrics.meter("Download failed");
    private static final Meter puts = metrics.meter("Put done");
//    private static final Meter organized = metrics.meter("Organized");
//    private static final Meter polite = metrics.meter("Polite links");
//    private static final Meter failedOrganize = metrics.meter("Failed to organize");
//    private static final Meter fetchedUrls = metrics.meter("Fetched urls");
//    private static final Meter canceledDownload = metrics.meter("Download canceled");

//    private static AtomicLong allUrlsSize = new AtomicLong(0);
//    private static AtomicLong downloadedSize = new AtomicLong(0);

    public static void start() {
//        metrics.register(MetricRegistry.name("all urls size"),
//                (Gauge<Long>) () -> allUrlsSize.get());
//
//        metrics.register(MetricRegistry.name("downloaded urls size"),
//                (Gauge<Long>) () -> downloadedSize.get());
//
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .build();

        reporter.start(1, TimeUnit.SECONDS);
    }

    public static void setAllUrlsSize(int size) {
//        allUrlsSize.set(size);
    }

    public static void setDownloadedSize(int size)
    {
//        downloadedSize.set(size);
    }

    /*****************************************************************************/

    public static void falseExistence()
    {
//        notExist.mark();
    }

    public static void exist()
    {
//        existNum.mark();
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
//        organized.mark();
    }

    public static void organizeFail() {
//        failedOrganize.mark();
    }

    public static void downloadFailed()
    {
//        failedDownload.mark();
    }

    public static void politeFound()
    {
//        polite.mark();
    }

    public static void fetched()
    {
//        fetchedUrls.mark();
    }

    public static void downloadCanceled()
    {
//        canceledDownload.mark();
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
