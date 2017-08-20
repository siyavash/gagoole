import Util.ContentTypeException;
import Util.LanguageDetector;
import Util.LanguageException;
import Util.UserAgents;
import javafx.util.Pair;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;

public class PageProcessor {

    private String pageUrl = "";
    private final Document document;

    PageProcessor(String url, Document document) { // throws LanguageException, ContentTypeException , HttpStatusException, SocketTimeoutException, IOException {
        pageUrl = url;
        this.document = document;
    }

    public ArrayList<Pair<String, String>> getAllInsideLinks() {

        ArrayList<Pair<String, String>> insideLinks = new ArrayList<Pair<String, String>>();
        Elements elements = document.getElementsByTag("a");
        if (elements != null) {
            for (Element tag : elements) {
                tag.attributes();
                String href = tag.absUrl("href");
                String anchor = tag.text();
                insideLinks.add(new Pair<String, String>(href, anchor));
            }
        }

        return insideLinks;
    }

    public URLData getUrlData() {
        URLData data = new URLData();
        data.setInsideLinks(getAllInsideLinks());
        data.setTitle(document.title());
        data.setPassage(document.body().text());
        data.setMeta(document.getElementsByTag("meta"));
        return data;
    }

//    public static void main(String[] args) throws Exception {
//        PageProcessor pageProcessor = new PageProcessor("https://jsoup.org/cookbook/extracting-data/working-with-urls");
//        System.out.println(pageProcessor.document.body().text());
//    }
}
