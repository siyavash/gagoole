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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataOrganizer
{
    private ArrayBlockingQueue<Pair<String, String>> downloadedData;
    private ArrayBlockingQueue<PageInfo> organizedData;
    private final int THREAD_NUMBER;

    public DataOrganizer(ArrayBlockingQueue<Pair<String, String>> downloadedData, ArrayBlockingQueue<PageInfo> organizedData)
    {
        this.downloadedData = downloadedData;
        this.organizedData = organizedData;
        THREAD_NUMBER = readProperty();
    }

    private int readProperty() {
        Properties prop = new Properties();

        try (InputStream input = new FileInputStream("config.properties"))
        {
            prop.load(input);
        } catch (IOException ex)
        {
            Profiler.error("Error while reading config file");
        }

        return Integer.parseInt(prop.getProperty("organizer-thread-number", "8"));
    }

    public void startOrganizing()
    {
        if (THREAD_NUMBER == 0)
        {
            return;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUMBER);

        for (int i = 0; i < THREAD_NUMBER; i++)
        {
            executorService.submit((Runnable) () -> {
                while (true)
                {
                    try
                    {
                        Pair<String, String> poppedData = downloadedData.take();


                        String text = poppedData.getKey();
                        String link = poppedData.getValue();

                        boolean isGoodContent = isGoodContent(link);
                        if (!isGoodContent)
                        {
                            Profiler.organizeFail();
                            continue;
                        }

                        if (text == null)
                        {
                            Profiler.organizeFail();
                            continue;
                        }


                        if (!isHtml(text))
                        {
                            Profiler.organizeFail();
                            continue;
                        }

                        Document dataDocument = createDataDocument(text);

                        if (!isEnglish(dataDocument))
                        {
                            Profiler.organizeFail();
                            continue;
                        }

                        PageInfo pageInfo = createPageInfo(dataDocument, link);
                        Profiler.organizeDone();

                        organizedData.put(pageInfo);

                    } catch (InterruptedException ignored)
                    {

                    }
                }
            });
        }

        executorService.shutdown();
    }

    private boolean isGoodContent(String url) {
        url = url.toLowerCase();
        return !url.endsWith(".jpg") && !url.endsWith(".gif") && !url.endsWith(".pdf") && !url.endsWith(".deb")
                && !url.endsWith(".jpeg") && !url.endsWith(".png") && !url.endsWith(".txt") && !url.endsWith(".exe")
                && !url.endsWith(".gz") && !url.endsWith(".rar") && !url.endsWith(".zip") && !url.endsWith(".tar.gz");

    }

    private PageInfo createPageInfo(Document dataDocument, String link)
    {
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
                href = normalizeUrl(href);
                if (isGoodContent(href) && !href.equals("") && !href.startsWith("mailto") && !href.startsWith("ftp"))
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
        if (normalizedUrl.endsWith("/") || normalizedUrl.endsWith(" "))
        {
            while (normalizedUrl.length()-slashCounter > 0 && (normalizedUrl.charAt(normalizedUrl.length()-slashCounter-1) == '/' || normalizedUrl.charAt(normalizedUrl.length()-slashCounter-1) == ' '))
                slashCounter++;
        }
        normalizedUrl = normalizedUrl.substring(0, normalizedUrl.length() - slashCounter);

        return normalizedUrl;
    }

    private Document createDataDocument(String text)
    {
        if(text == null)
            return null;

        return Jsoup.parse(text);
    }

    private boolean isEnglish(Document dataDocument)
    {
        LanguageDetector languageDetector = new LanguageDetector(dataDocument);

        return languageDetector.isEnglish();
    }

    private boolean isHtml(String text)
    {
        text = text.toLowerCase();
        boolean result = text.contains("<body")/* && text.contains("</html>")*/;

        return result;
    }
}