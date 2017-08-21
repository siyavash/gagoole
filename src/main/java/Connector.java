import Util.*;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;

public class Connector {

    private String pageUrl;
    private Document document;

    public Connector(String pageUrl) throws ContentTypeException, LanguageException, SocketTimeoutException, Exception { // TODO: make this class functions for crawlThread
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

    private void checkContentType() throws ContentTypeException, SocketTimeoutException, Exception {
        String contentType = getContentType();
        if (contentType != null && !contentType.startsWith("text/html"))
            throw new ContentTypeException(pageUrl);
    }

    private void checkLanguage() throws LanguageException {
        LanguageDetector languageDetector = new LanguageDetector(document);
        if (!languageDetector.isEnglish())
            throw new LanguageException(pageUrl);
    }

    private Document getDocumentFromConnection() throws SocketTimeoutException, IOException {
        try {
            Connection.Response response = Jsoup.connect(pageUrl)
                    .userAgent(UserAgents.getRandom())
                    .maxBodySize(100 * 1024)
                    .execute();
            return response.parse();
        } catch (SocketTimeoutException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        }
    }

    private String getContentType() throws SocketTimeoutException, Exception {
        try {
            URL url = new URL(pageUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.connect();
            return connection.getContentType();
        } catch (SocketTimeoutException e) {
            throw e;
        }
    }
}