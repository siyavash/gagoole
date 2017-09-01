package util;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class LanguageDetector {
    private Document doc;

    public LanguageDetector(Document document) { doc = document; }

    public boolean isEnglish()
    {
        return !checkLangAttribute() && (/*checkTitle() &&*/ checkAllContent());

    }

    private boolean stringChecker(String str) {
        final int ENGLISH_CHARS_MAX_VALUE = 128;
        double nonEnglishChars = 0;
        double allChars = 0;

        for (int i = 0; i < str.length(); i++) {
            allChars++;
            if (str.charAt(i) >= ENGLISH_CHARS_MAX_VALUE)
                nonEnglishChars++;
        }

        return !(nonEnglishChars > 0.02 * allChars);
    }

//    private boolean checkTitle() {
//        String title = doc.title();
//        return stringChecker(title);
//    }

    private boolean checkLangAttribute() {
        try {
            Element htmlTag = doc.getElementsByTag("html").first();
            String lang = htmlTag.attr("lang");
            if (lang != null && !lang.equals("")) {
                return !(lang.startsWith("en") || lang.equals("mul"));
            } else {
                return false;
            }
        } catch (NullPointerException e) {
            return false;
        }
    }

    private boolean checkAllContent() {
        return stringChecker(doc.text());
    }

}
