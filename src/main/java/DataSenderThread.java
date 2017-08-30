import datastore.DataStore;
import datastore.PageInfo;
import javafx.util.Pair;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import queue.URLQueue;
import util.LanguageDetector;
import util.Profiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class DataSenderThread extends Thread
{
    private DataStore dataStore;
    private URLQueue urlQueue;
    private ArrayBlockingQueue<Pair<String, String>> downloadedDataBlockingQueue;

    public DataSenderThread(DataStore dataStore, URLQueue urlQueue, ArrayBlockingQueue<Pair<String, String>> downloadedDataBlockingQueue)
    {
        this.dataStore = dataStore;
        this.urlQueue = urlQueue;
        this.downloadedDataBlockingQueue = downloadedDataBlockingQueue;
    }

    @Override
    public void run()
    {
        while (true)
        {
            long t1 = System.currentTimeMillis();

            String pageData = null;
            String link = null;

            try
            {
                Pair<String, String> downloadedData = downloadedDataBlockingQueue.take();
                pageData = downloadedData.getKey();
                link = downloadedData.getValue();

            } catch (InterruptedException e)
            {
                e.printStackTrace(); //TODO
            }

            Document parsedData = parseData(pageData, link);
            if(parsedData == null)
                continue;
            if (!isEnglish(parsedData, link))
            {
                continue;
            }
            PageInfo pageInfo = createPageInfo(link, parsedData);
            storeInDataStore(pageInfo, link);
            pushSubLinksToQueue(pageInfo, link);

            t1 = System.currentTimeMillis() - t1;
            Profiler.dataSentLog(link, t1);
        }
    }

    private void pushSubLinksToQueue(PageInfo pageInfo, String link)
    {
        long t1 = System.currentTimeMillis();
        ArrayList<String> subLinks = getAllSubLinksFromPageInfo(pageInfo);

        urlQueue.push(subLinks);

        t1 = System.currentTimeMillis() - t1;
        Profiler.pushToQueue(link, t1);
    }

    private ArrayList<String> getAllSubLinksFromPageInfo(PageInfo pageInfo)
    {
        ArrayList<String> subLinks = new ArrayList<>();
        for (Pair<String, String> pair : pageInfo.getSubLinks())
        {
            subLinks.add(pair.getKey());
        }
        return subLinks;
    }

    private void storeInDataStore(PageInfo pageInfo, String link)
    {
        long t1 = System.currentTimeMillis();

        try
        {
            dataStore.put(pageInfo);
        } catch (IOException e)
        {
            e.printStackTrace(); //TODO
        }

        t1 = System.currentTimeMillis() - t1;
        Profiler.putToDataStore(link, t1);
    }

    private PageInfo createPageInfo(String link, Document parsedData)
    {
        long t1 = System.currentTimeMillis();
        PageInfo pageInfo = new PageInfo();

        pageInfo.setUrl(normalizeUrl(link));
        pageInfo.setSubLinks(getAllSubLinksWithAnchor(parsedData));
        pageInfo.setTitle(parsedData.title());

        if (parsedData.body() != null)
        {
            pageInfo.setBodyText(parsedData.body().text());
        }

        Elements elements = parsedData.select("meta[name=author]");
        if (elements != null)
        {
            pageInfo.setAuthorMeta(elements.attr("content"));
        }

        elements = parsedData.select("meta[name=description]");
        if (elements != null)
        {
            pageInfo.setDescriptionMeta(elements.attr("content"));
        }

        elements = parsedData.select("meta[name=content-type]");
        if (elements != null)
        {
            pageInfo.setContentTypeMeta(elements.attr("content"));
        }

        elements = parsedData.select("meta[name=keywords]");
        if (elements != null)
        {
            pageInfo.setKeyWordsMeta(elements.attr("content"));
        }

        t1 = System.currentTimeMillis() - t1;
        Profiler.extractInformationFromDocument(link, t1);

        return pageInfo;
    }

    private ArrayList<Pair<String, String>> getAllSubLinksWithAnchor(Document parsedData)
    {
        ArrayList<Pair<String, String>> subLinks = new ArrayList<>();
        Elements elements = parsedData.getElementsByTag("a");
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

    private boolean isEnglish(Document parsedData, String link)
    {
        long t1 = System.currentTimeMillis();

        LanguageDetector languageDetector = new LanguageDetector(parsedData);
        boolean englishLanguage = languageDetector.isEnglish();

        t1 = System.currentTimeMillis() - t1;
        Profiler.goodLanguage(link, t1, englishLanguage);

        return englishLanguage;
    }

    private Document parseData(String pageData, String link)
    {
        long t1 = System.currentTimeMillis();
        if(pageData == null)
            return null;
        Document document = Jsoup.parse(pageData);

        t1 = System.currentTimeMillis() - t1;
        Profiler.parse(link, t1);

        return document;
    }
}
