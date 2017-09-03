import datastore.PageInfo;
import javafx.util.Pair;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import util.LanguageDetector;
import util.Profiler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class DataOrganizer
{
    private ArrayBlockingQueue<Pair<String, String>> downloadedData;
    private ArrayBlockingQueue<PageInfo> organizedData;
    private final int THREAD_NUMBER;

    public DataOrganizer(ArrayBlockingQueue<Pair<String, String>> downloadedData, ArrayBlockingQueue<PageInfo> organizedData)
    {
        this.downloadedData = downloadedData;
        this.organizedData = organizedData;
        Properties prop = new Properties();
        InputStream input = null;

        try
        {
            input = new FileInputStream("config.properties");
            prop.load(input);
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

        THREAD_NUMBER = Integer.parseInt(prop.getProperty("organizer-thread-number", "8"));
    }

    public void startOrganizing()
    {
        if (THREAD_NUMBER == 0)
        {
            return;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUMBER);
//        AtomicInteger atomicInteger = new AtomicInteger(0);
//        Timer timer = new Timer();
//        timer.scheduleAtFixedRate(new TimerTask() {
//            @Override
//            public void run() {
//                System.out.println("organized : " + atomicInteger.get());
//                atomicInteger.set(0);
//            }
//        }, 0, 1000);
        for (int i = 0; i < THREAD_NUMBER; i++)
        {
            executorService.submit((Runnable) () -> {
                while (true)
                {
                    try
                    {
//                        long time = System.currentTimeMillis();
                        Pair<String, String> poppedData = popNewData();


                        String text = poppedData.getKey();
                        String link = poppedData.getValue();

                        if (text == null)
                        {
                            continue;
                        }
                        Profiler.putDone(1);


                        if (!isHtml(text, link))
                        {
                            continue;
                        }

                        Document dataDocument = createDataDocument(text, link);

                        if (!isEnglish(dataDocument, link))
                        {
                            continue;
                        }

                        PageInfo pageInfo = createPageInfo(dataDocument, link);

                        sendOrganizedData(pageInfo);


//                        time = System.currentTimeMillis() - time;
//                        Profiler.organized(link, time);
                    } catch (InterruptedException ignored)
                    {
                        //TODO is this enough?
                    } /*catch (Exception e)*/
//                    {
//                        e.printStackTrace();
//                    }
//                    atomicInteger.incrementAndGet();
                }
            });
        }

        executorService.shutdown();
    }

    private void sendOrganizedData(PageInfo pageInfo) throws InterruptedException
    {
//        long time = System.currentTimeMillis();

        organizedData.put(pageInfo);

//        time = System.currentTimeMillis() - time;
//        Profiler.putOrganizedData(pageInfo.getUrl(), time);
    }

    private PageInfo createPageInfo(Document dataDocument, String link)
    {
//        long time = System.currentTimeMillis();

        PageInfo pageInfo = new PageInfo();

        pageInfo.setUrl(normalizeUrl(link));
        pageInfo.setSubLinks(getAllSubLinksWithAnchor(dataDocument));
        pageInfo.setTitle(dataDocument.title());

        if (dataDocument.body() != null)
        {
            pageInfo.setBodyText(dataDocument.body().text());
        }

        Elements elements = dataDocument.select("meta[name=author]");
        if (elements != null)
        {
            pageInfo.setAuthorMeta(elements.attr("content"));
        }

        elements = dataDocument.select("meta[name=description]");
        if (elements != null)
        {
            pageInfo.setDescriptionMeta(elements.attr("content"));
        }

        elements = dataDocument.select("meta[name=content-type]");
        if (elements != null)
        {
            pageInfo.setContentTypeMeta(elements.attr("content"));
        }

        elements = dataDocument.select("meta[name=keywords]");
        if (elements != null)
        {
            pageInfo.setKeyWordsMeta(elements.attr("content"));
        }

//        time = System.currentTimeMillis() - time;
//        Profiler.extractInformationFromDocument(link, time);

        return pageInfo;
    }

    private ArrayList<Pair<String, String>> getAllSubLinksWithAnchor(Document dataDocument)
    {
        ArrayList<Pair<String, String>> subLinks = new ArrayList<>();
        Elements elements = dataDocument.getElementsByTag("a");
        if (elements != null)
        {
            for (Element tag : elements)
            {
                String href = tag.absUrl("href");
                if (!href.equals("") && !href.startsWith("mailto") && !href.startsWith("ftp"))
                {
                    String anchor = tag.text();
                    subLinks.add(new Pair<>(href, anchor));
                }
            }
        }

        return subLinks;
    }

    private String normalizeUrl(String link)
    {
        String normalizedUrl = link.replaceFirst("(www\\.)", "");

        int slashCounter = 0;
        if (normalizedUrl.endsWith("/"))
        {
            while (normalizedUrl.length() - slashCounter > 0 && normalizedUrl.charAt(normalizedUrl.length() - slashCounter - 1) == '/')
                slashCounter++;
        }
        normalizedUrl = normalizedUrl.substring(0, normalizedUrl.length() - slashCounter);

        return normalizedUrl;
    }

    private Document createDataDocument(String text, String link)
    {
//        long time = System.currentTimeMillis();

        if(text == null)
            return null;
        Document document = Jsoup.parse(text);

//        time = System.currentTimeMillis() - time;
//        Profiler.parse(link, time);

        return document;
    }

    private Pair<String, String> popNewData() throws InterruptedException
    {
//        long time = System.currentTimeMillis();

        Pair<String, String> poppedData = downloadedData.take();

//        time = System.currentTimeMillis() - time;
//        Profiler.popDownloadedData(poppedData.getValue(), time);

        return poppedData;
    }

    private boolean isEnglish(Document dataDocument, String link)
    {
//        long time = System.currentTimeMillis();

        LanguageDetector languageDetector = new LanguageDetector(dataDocument);
        boolean englishLanguage = languageDetector.isEnglish();

//        time = System.currentTimeMillis() - time;
//        Profiler.goodLanguage(link, time, englishLanguage);

        return englishLanguage;
    }

    private boolean isHtml(String text, String link)
    {
//        long time = System.currentTimeMillis();

        text = text.toLowerCase();
        boolean result = text.contains("<body")/* && text.contains("</html>")*/;

//        time = System.currentTimeMillis() - time;
//        Profiler.htmlCheck(link, time);

        return result;
    }
}
