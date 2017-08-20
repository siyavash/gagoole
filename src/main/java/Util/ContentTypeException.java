package Util;

public class ContentTypeException extends Exception {
    String url;

    public ContentTypeException(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }
}
