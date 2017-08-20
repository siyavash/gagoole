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

    PageProcessor(String url) throws LanguageException, ContentTypeException , HttpStatusException, SocketTimeoutException, IOException {
        pageUrl = url;
        checkContentType();
        document = getDocument();
        checkLanguage(document);
    }

    public ArrayList<Pair<String, String>> getAllInsideLinks() {

        ArrayList<Pair<String, String>> insideLinks = new ArrayList<>();
        Elements elements = document.getElementsByTag("a");
        if (elements != null) {
            for (Element tag : elements) {
                tag.attributes();
                String href = tag.absUrl("href");
                String anchor = tag.text();
                insideLinks.add(new Pair<>(href, anchor));
            }
        }

        return insideLinks;
    }

    private void checkContentType() throws ContentTypeException {
        String contentType = getContentType();
        if (contentType != null && !contentType.startsWith("text/html"))
            throw new ContentTypeException(pageUrl);
    }

    private void checkLanguage(Document document) throws LanguageException {
        LanguageDetector languageDetector = new LanguageDetector(document);
        if (!languageDetector.isEnglish())
            throw new LanguageException(pageUrl);
    }

    private Document getDocument() {
        try {
            Connection.Response response = Jsoup.connect(pageUrl)
                    .userAgent(getRandomUserAgent())
                    .maxBodySize(100 * 1024)
                    .execute();
            return response.parse();
        } catch (IOException e) {
            System.err.println("error in connecting url " + pageUrl);
            e.printStackTrace();
            return null;
        }
    }

    private String getContentType() {
        try {
            URL url = new URL(pageUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.connect();
            return connection.getContentType();
        } catch (Exception e) {
            System.err.println("gand" + pageUrl);
            return null;
        }
    }

    private static String getRandomUserAgent() {
        return UserAgents.getRandom();
    }

    public URLData getUrlData() {
        URLData data = new URLData();
        data.setInsideLinks(getAllInsideLinks());
        data.setTitle(document.title());
        data.setPassage(document.body().text());
        // TODO: data.setMeta();
        return data;
    }

//    public static void main(String[] args) throws Exception {
//        PageProcessor pageProcessor = new PageProcessor("https://jsoup.org/cookbook/extracting-data/working-with-urls");
//        System.out.println(pageProcessor.document.body().text());
//    }
}
