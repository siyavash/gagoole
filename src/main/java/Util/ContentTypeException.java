package Util;

/**
 * Created by Amir on 8/20/2017 AD.
 */
public class ContentTypeException extends Exception {
    String url;

    public ContentTypeException(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }
}
