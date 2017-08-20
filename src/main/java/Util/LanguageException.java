package Util;

public class LanguageException extends Exception {
    private String url;

    public LanguageException(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }
}
