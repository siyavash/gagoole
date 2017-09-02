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
    private static final Meter fetchedFromQueue = metrics.meter("fetched from queue");
    private static final Meter polities = metrics.meter("polite links");
    private static final Meter goodLanguage = metrics.meter("good language");
    private static final Meter crawled = metrics.meter("crawled");
    private static final Meter goodContentType= metrics.meter("good content type");

    private static AtomicLong queueSize = new AtomicLong(0);
    private static AtomicLong properSize = new AtomicLong(0);
    private static AtomicLong newUrlsSize = new AtomicLong(0);
    private static AtomicLong downloadedSize = new AtomicLong(0);

    public static void start() {
        metrics.register(MetricRegistry.name("initial kafka queue size"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queueSize.get();
                    }
                });

        metrics.register(MetricRegistry.name("proper urls queue size"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return properSize.get();
                    }
                });

        metrics.register(MetricRegistry.name("new urls doesn't exist in hbase"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return newUrlsSize.get();
                    }
                });

        metrics.register(MetricRegistry.name("downloade data queue size"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return downloadedSize.get();
                    }
                });

        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .build();

        reporter.start(1, TimeUnit.SECONDS);
    }




    public static void setQueueSize(long size) {
        queueSize.set(size);
    }

    public static void setPropersSize(long size) {
        properSize.set(size);
    }

    public static void setDownloadedSize(long size) {
        downloadedSize.set(size);
    }

    public static void setNewUrlsSize(int size) {
        newUrlsSize.set(size);
    }


    /*****************************************************************************/


    public static void getLinkFromKafkaQueue(String url, long time) {
        logger.info(String.format("Link got from kafka to check if polite queue in time %d: %s", time, url));
        fetchedFromQueue.mark();
    }

    public static void checkPolitensess(String url, long time, boolean isPolite) {
        String politeness = (isPolite ? "is polite" : "not polite");
        logger.info(String.format("Checked Politeness (%s) in time %d: %s", politeness, time, url));
        if (isPolite) polities.mark();
    }

    public static void checkContentType(String url, long time, boolean isGood) {
        logger.info(String.format("Checked content type (%s) in time %d: %s", isGood, time, url));
        if (isGood) goodContentType.mark();
    }

    public static void pushUrlToProperQueue(String urlToVisit, long singleFetchingTaskTime) {
        logger.info("Push Url into proper Urls time: " + singleFetchingTaskTime + ", link: " + urlToVisit);
    }

    public static void getLinkFinished(String linkToVisit, long timeDifference) {
        logger.info(String.format("fetched link to check if is new %d: %s", timeDifference, linkToVisit));
    }

    /** FIRST THREAD FINISHED **/

    public static void getUrlToCheckIfNew(String urlToVisit, long singleCheckingTime){
        logger.info("Proper Url is ready to be checked time: " + singleCheckingTime + ", link: " + urlToVisit);
    }

    public static void checkedExistance(String urlToVisit, long singleCheckingTime){
        logger.info("Single Url is checked time: " + singleCheckingTime + ", link: " + urlToVisit);
    }

    public static void checkAllExistanceTaskTime(String url, long time, boolean isExists) {
        String existence = (isExists ? "exists" : "does'nt exist");
        logger.info(String.format("Check existence in data store (%s) in time %d: %s", existence, time, url));
    }

    /** SECOND THREAD FINISHED **/


    public static void getLinkFromQueueToDownload(String url, long singleDownloadingTaskTime) {
        logger.info("Url got from new Url queue to Download: " + singleDownloadingTaskTime + ", link: " + url);
    }

    public static void download(String url, long time) {
        logger.info(String.format("Downloaded in time %d: %s", time, url));
    }

    public static void pushBackToKafka(String link, long time) {
        logger.info("Error occurred after download and pushed back to kafka in time: " + time + ", link: " + link);
    }

    public static void putUrlBody(String url, long singleDownloadingTaskTime) {
        logger.info("Body got : " + singleDownloadingTaskTime + ", link: " + url);
    }

    public static void downloadThread(String link, long downloadTime) {
        logger.info("Download thread done in time " + downloadTime + ", link: " + link);
    }

    /** THIRD THREAD FINISHED **/

    public static void organized(String link, long time)
    {
        logger.info("Organizing thread finished in time: " + time + ", link" + link);
    }

    public static void extractInformationFromDocument(String url, long time) {
        logger.info(String.format("Extracted info from document in time %d: %s", time, url));
        crawled.mark();
    }

    public static void parse(String url, long time) {
        logger.info(String.format("Parsed in time %d: %s", time, url));
    }

    public static void popDownloadedData(String link, long time)
    {
        logger.info("Popped data from downloaded data queue in time: " + time + ", link: " + link);
    }

    public static void goodLanguage(String url, long time, boolean isEnglish) {
        String beingEnglish = (isEnglish ? "is english" : "not english");
        logger.info(String.format("Checked good language (%s) in time %d: %s", beingEnglish, time, url));
        if (isEnglish) goodLanguage.mark();
    }

    public static void htmlCheck(String link, long time) {
        logger.info("Html checked in time: " + time + ", link: " + link);
    }

    /** FOURTH THREAD FINISHED **/


    public static void dataSentLog(String link, long sentTime) {
        logger.info("Data sent to hbase and kafka in time " + sentTime + ", link: " + link);
    }

    public static void pushToQueue(String url, long time) {
        logger.info(String.format("Pushed to queue in time: %d %s", time, url));
    }

    public static void putToDataStore(String url, long time) {
        logger.info(String.format("Putted in data store in time %d: %s", time, url));
    }


    public static void putOrganizedData(String link, long time)
    {
        logger.info("Sent PageInfo object to organized queue in time: " + time + ", link: " + link);
    }

    public static void popOrganizedData(String link, long time)
    {
        logger.info("Popped data from organized queue in time: " + time + ", link: " + link);
    }


    
    /** UNUSED **/


    public static void crawled(String url, long time) {
        logger.info(String.format("Completely crawled in time %d: %s", time, url));
    }


    public static void writeRequestTimeLog(long requestTime, String link) {
        logger.info("Request time is " + requestTime + ", for link : " + link);
    }

    public static void writeResponseTimeLog(long responseTime, String link) {
        logger.info("Response time is " + responseTime + ", for link : " + link);
    }

    public static void writeCheckHeaderTimeLog(long checkHeaderTime, String link) {
        logger.info("Checked header in " + checkHeaderTime + ", for link : " + link);
    }

    public static void logGapTime(String link, long gapTime) {
        logger.info("Gap time after download html and before put to downloaded queue: " + gapTime + ", link: " + link);
    }








}
