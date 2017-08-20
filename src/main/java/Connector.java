import Util.*;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Amir on 8/20/2017 AD.
 */
public class Connector {

    private String pageUrl;
    private Document document;

    public Connector(String pageUrl) throws ContentTypeException, LanguageException {
        this.pageUrl = pageUrl;

        checkContentType();
        Logger.goodContentType();

        document = getDocumentFromConnection();
        checkLanguage();
        Logger.goodLanguage();
    }

    public Document getDocument() {
        return document;
    }

    private void checkContentType() throws ContentTypeException {
        String contentType = getContentType();
        if (contentType != null && !contentType.startsWith("text/html"))
            throw new ContentTypeException(pageUrl);
    }

    private void checkLanguage() throws LanguageException {
        LanguageDetector languageDetector = new LanguageDetector(document);
        if (!languageDetector.isEnglish())
            throw new LanguageException(pageUrl);
    }

    private Document getDocumentFromConnection() {
        try {
            Connection.Response response = Jsoup.connect(pageUrl)
                    .userAgent(UserAgents.getRandom())
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
            System.err.println("error in getting content-type" + pageUrl);
            return null;
        }
    }
}