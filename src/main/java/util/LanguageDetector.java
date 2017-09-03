package util;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class LanguageDetector {
    private Document doc;

    public LanguageDetector(Document document) { doc = document; }

    public boolean isEnglish() {
        if (langAttrIsNotEnglish())
            return false;

        return checkAllContent();
    }

    private boolean stringChecker(String str) {
        final int ENCHARS = 128;
        int nonEnglishChars = 0;
        int allChars = 0;

        for (int i = 0; i < str.length(); i++) {
            allChars++;
            if (str.charAt(i) >= ENCHARS)
                nonEnglishChars++;
        }

        if (nonEnglishChars > 0.02 * allChars) {
            return false;
        } else {
            return true;
        }
    }

    private boolean langAttrIsNotEnglish() {
        try {
            Element htmlTag = doc.getElementsByTag("html").first();
            String lang = htmlTag.attr("lang");
            if (lang != null && !lang.equals("")) {
                return !(lang.startsWith("en") || lang.equals("mul"));
            } else {
                return true;
            }
        } catch (NullPointerException e) {
            return true;
        }
    }

    private boolean checkAllContent() {
        return stringChecker(doc.text());
    }

}
